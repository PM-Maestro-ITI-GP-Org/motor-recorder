#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/neutrino.h>
#include <sys/wait.h>

#include "motor_wire.h"
#include "motor_shm.h"
#include "mqtt_client.h"

#define POLL_TIMEOUT_MS   5
#define ROW_BUF_SIZE      4096
#define QUEUE_MASK        0x3FFFFu
#define DEFAULT_DIR       "/tmp"

static char g_save_dir[512] = DEFAULT_DIR;


/* ======== Data types ======== */

struct row_buf {
    char data[ROW_BUF_SIZE];
    size_t len;
};

typedef struct {
    uint64_t     ts;
    motor_row_t  row;
} csv_row_t;

typedef struct {
    csv_row_t          *buf;
    _Atomic uint32_t    head;
    _Atomic uint32_t    tail;
    uint32_t            mask;
} row_queue_t;

typedef struct {
    const shm_region_t *region;
    uint64_t            read_pos;
    uint64_t            dropped_blocks;
} shm_state_t;

typedef struct reader_s {
    shm_state_t  *shm;
    row_queue_t  *queue;
    volatile bool active;
    volatile bool done;
    uint64_t  first_ts;
    uint64_t  last_ts;
    uint64_t  block_count;
    uint32_t  freq_hz;
    uint32_t  interval_us;
    uint16_t  block_n_rows;
    bool      meta_seen;
    bool      first_seen;
    uint64_t  dropped;
    uint64_t  total_rows;
    uint64_t  last_wp;
    uint64_t  stalled_ms;
} reader_t;

typedef struct writer_s {
    row_queue_t  *queue;
    FILE         *csv_file;
    volatile bool active;
    volatile bool done;
    uint64_t     *rec_rows_ptr;
} writer_t;

/* ======== Forward declarations ======== */

static row_queue_t *queue_alloc(uint32_t power_of_two);
static void stop_recording(void);
static void queue_free(row_queue_t *q);
static bool queue_push(row_queue_t *q, const csv_row_t *r);
static bool queue_pop(row_queue_t *q, csv_row_t *r);
static void row_buf_add(struct row_buf *b, uint64_t ts, const motor_row_t *row);
static void row_buf_flush(FILE *f, struct row_buf *b);
static void *reader_thread(void *arg);
static void *writer_thread(void *arg);
static shm_state_t *shm_try_connect(void);
static void shm_disconnect(shm_state_t *s);
static size_t shm_poll(shm_state_t *s, shm_block_t *out, size_t max_blocks);
static FILE *open_csv(const char *path);
static void generate_filename(char *buf, size_t bufsz);
static void recorder_cleanup(void);
static void handle_command(mqtt_client_t *m, const char *cmd);

/* ======== Globals ======== */

static volatile bool g_running = true;
static volatile bool g_recording = false;
static char g_csv_filename[1024] = "";
static uint64_t g_rec_rows = 0;
static pthread_mutex_t g_rec_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t g_rd_thr, g_wr_thr;
static FILE *g_csv_file = NULL;
static row_queue_t *g_queue = NULL;
static shm_state_t *g_shm = NULL;
static reader_t *g_rd = NULL;
static writer_t *g_wr = NULL;
static mqtt_client_t *g_mqtt = NULL;
static uint64_t g_stop_span_us = 0;
static uint64_t g_stop_total_rows = 0;
static uint64_t g_stop_drops = 0;
static uint64_t g_stop_block_drops = 0;
static uint64_t g_stop_stalled_ms = 0;
static bool g_has_stop_data = false;
static bool g_uploading = false;

static uint64_t g_auto_stop_time = 0;
static bool g_auto_stop_active = false;

/* Waiting for the data producer (SHM /motor_ctrl) to come up so a queued
   "start" can actually begin. While set, incoming commands get a
   "producer is off (no data)" reply. */
static bool g_waiting_for_producer = false;
static char g_pending_filename[1024] = "";
static uint64_t g_pending_duration = 0;

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* ======== Filenames arriving over MQTT ======== */

/*
 * Turn a filename from a command into a path inside g_save_dir, or refuse.
 *
 * Every one of these names arrives over MQTT, and the broker credentials are
 * compiled into this repository, so "whoever can reach the broker" is the
 * threat model whether or not that was intended. Before this, `delete` pasted
 * the name straight into "%s/%s" -- so `delete ../../etc/passwd` resolved
 * wherever the ".." took it -- and `download` was worse: it looked for a '/'
 * and, finding one, used the argument as an absolute path verbatim, which made
 * `download /etc/shadow` a supported way to read any file on the guest out over
 * MQTT.
 *
 * A recording filename is a single directory entry by construction
 * (generate_filename produces one, and the list command only ever reports
 * one), so requiring exactly that costs nothing and ends the whole class:
 * no separators, no "..", no leading dot, no empty name.
 *
 * Returns 0 on success.
 */
static int resolve_in_save_dir(const char *name, char *out, size_t out_sz)
{
    if (!name || !*name) return -1;
    if (strchr(name, '/') || strchr(name, '\\')) return -1;
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) return -1;
    if (name[0] == '.') return -1;              /* no dotfiles, no "..foo" */
    if (strlen(name) > 255) return -1;

    int n = snprintf(out, out_sz, "%s/%s", g_save_dir, name);
    if (n < 0 || (size_t)n >= out_sz) return -1;
    return 0;
}

/*
 * Escape a string for embedding in a JSON double-quoted value.
 *
 * Filenames were pasted in raw. One containing a quote or a backslash -- which
 * nothing prevents, since `start <name>` takes the name from the command --
 * produced a payload the GUI's JSON.parse() rejected, and the reply was
 * dropped whole rather than being shown wrong. Returns the number of bytes
 * written, and always terminates.
 */
static size_t json_escape(const char *in, char *out, size_t out_sz)
{
    size_t j = 0;
    if (out_sz == 0) return 0;
    for (size_t i = 0; in && in[i] && j + 8 < out_sz; i++) {
        unsigned char c = (unsigned char)in[i];
        switch (c) {
        case '"':  out[j++] = '\\'; out[j++] = '"';  break;
        case '\\': out[j++] = '\\'; out[j++] = '\\'; break;
        case '\n': out[j++] = '\\'; out[j++] = 'n';  break;
        case '\r': out[j++] = '\\'; out[j++] = 'r';  break;
        case '\t': out[j++] = '\\'; out[j++] = 't';  break;
        default:
            if (c < 0x20) j += (size_t)snprintf(out + j, out_sz - j, "\\u%04x", c);
            else          out[j++] = (char)c;
        }
    }
    out[j] = '\0';
    return j;
}

/* Does `name` end in ".csv"? strstr() matched the extension anywhere, so
   "run.csv.bak" and "notes.csvx" were both listed as recordings. */
static bool has_csv_suffix(const char *name)
{
    size_t n = strlen(name);
    return n > 4 && strcmp(name + n - 4, ".csv") == 0;
}

/* ======== Queue ======== */

static row_queue_t *queue_alloc(uint32_t power_of_two)
{
    uint32_t cap = 1u << power_of_two;
    row_queue_t *q = calloc(1, sizeof(*q));
    if (!q) return NULL;
    q->buf = calloc(cap, sizeof(csv_row_t));
    if (!q->buf) { free(q); return NULL; }
    q->mask = cap - 1;
    return q;
}

static void queue_free(row_queue_t *q)
{
    if (!q) return;
    free(q->buf);
    free(q);
}

static bool queue_push(row_queue_t *q, const csv_row_t *r)
{
    uint32_t h = atomic_load_explicit(&q->head, memory_order_relaxed);
    uint32_t t = atomic_load_explicit(&q->tail, memory_order_acquire);
    uint32_t nxt = (h + 1) & q->mask;
    if (nxt == t) return false;
    q->buf[h] = *r;
    atomic_store_explicit(&q->head, nxt, memory_order_release);
    return true;
}

static bool queue_pop(row_queue_t *q, csv_row_t *r)
{
    uint32_t h = atomic_load_explicit(&q->head, memory_order_acquire);
    uint32_t t = atomic_load_explicit(&q->tail, memory_order_relaxed);
    if (h == t) return false;
    *r = q->buf[t];
    atomic_store_explicit(&q->tail, (t + 1) & q->mask, memory_order_release);
    return true;
}

/* ======== Row buffer ======== */

static void row_buf_add(struct row_buf *b, uint64_t ts, const motor_row_t *row)
{
    int n = snprintf(b->data + b->len, sizeof(b->data) - b->len,
                     "%llu,%u,%u,%u,%u,%u,%u,%u,%u,%d,%d,%d,%u\n",
                     (unsigned long long)ts,
                     row->current[0], row->current[1], row->current[2], row->current[3],
                     row->current[4], row->current[5], row->current[6], row->current[7],
                     row->vib_x, row->vib_y, row->vib_z, row->rpm);
    if (n > 0) b->len += (size_t)n;
}

static void row_buf_flush(FILE *f, struct row_buf *b)
{
    if (b->len > 0) {
        fwrite(b->data, 1, b->len, f);
        b->len = 0;
    }
}

/* ======== Threads ======== */

static void pin_to_cpu(int cpu)
{
    uint64_t runmask = 1ULL << cpu;
    ThreadCtl(_NTO_TCTL_RUNMASK, &runmask);
}

static void *reader_thread(void *arg)
{
    pin_to_cpu(0);
    reader_t *rd = (reader_t *)arg;
    rd->dropped = 0;
    rd->stalled_ms = 0;

    while (g_running && g_recording && rd->active) {
        struct timespec ts = {0, POLL_TIMEOUT_MS * 1000 * 1000L};
        nanosleep(&ts, NULL);

        shm_block_t blocks[16];
        size_t n = shm_poll(rd->shm, blocks, 16);
        uint64_t wp = motor_ring_write_pos(&rd->shm->region->ring);
        if (wp != rd->last_wp) {
            rd->last_wp = wp;
            rd->stalled_ms = 0;
        } else {
            rd->stalled_ms += POLL_TIMEOUT_MS;
            if (rd->stalled_ms == 5000)
                fprintf(stderr, "\n[rec] WARNING: producer stalled for 5s (wp=%llu)\n",
                        (unsigned long long)wp);
        }
        for (size_t i = 0; i < n; ++i) {
            rd->total_rows += blocks[i].n_rows;
            for (uint16_t j = 0; j < blocks[i].n_rows; ++j) {
                uint64_t ts = blocks[i].row_ts[j];
                if (!rd->first_seen) {
                    rd->first_ts = ts;
                    rd->first_seen = true;
                }
                rd->last_ts = ts;
                csv_row_t cr = { .ts = ts, .row = blocks[i].rows[j] };
                if (!queue_push(rd->queue, &cr))
                    rd->dropped++;
            }
        }
    }

    rd->done = true;
    return NULL;
}

/*
 * How often a sample row is published for the GUI's live display.
 *
 * The rows themselves arrive far faster than this -- the producer streams a
 * block at a time off SPI -- and every one of them still goes to the CSV. This
 * governs only the copy sent over MQTT, which exists to drive a panel a person
 * is looking at. Ten a second is past the point where a number on screen reads
 * as anything but a blur, and publishing every row instead would put thousands
 * of messages a second onto a broker roughly 145ms away.
 */
#define LIVE_ROW_INTERVAL_MS 100

static void *writer_thread(void *arg)
{
    pin_to_cpu(1);
    writer_t *w = (writer_t *)arg;
    struct row_buf buf = { .len = 0 };
    uint64_t last_pub_ms = 0;

    while (g_running && g_recording && w->active) {
        csv_row_t cr;
        if (queue_pop(w->queue, &cr)) {
            row_buf_add(&buf, cr.ts, &cr.row);
            if (w->rec_rows_ptr) {
                pthread_mutex_lock(&g_rec_mutex);
                (*w->rec_rows_ptr)++;
                pthread_mutex_unlock(&g_rec_mutex);
            }

            /*
             * Publish a sample for the live display.
             *
             * mqtt_client_publish_row() existed, was declared in the header,
             * and was called from nowhere at all -- so the recorder wrote every
             * row to disk and sent not one of them to the GUI. The GUI has had
             * a handler for the data topic the whole time, splitting the 13
             * fields this publishes; it simply never received anything, which
             * is why the live panel sat at "---" throughout a recording.
             */
            uint64_t now = now_ms();
            if (g_mqtt && now - last_pub_ms >= LIVE_ROW_INTERVAL_MS) {
                last_pub_ms = now;
                mqtt_client_publish_row(g_mqtt, cr.ts, &cr.row);
            }

            if (buf.len >= ROW_BUF_SIZE / 2)
                row_buf_flush(w->csv_file, &buf);
        } else {
            if (buf.len > 0)
                row_buf_flush(w->csv_file, &buf);
            struct timespec ts = {0, 1 * 1000 * 1000L};
            nanosleep(&ts, NULL);
        }
    }

    if (buf.len > 0)
        row_buf_flush(w->csv_file, &buf);
    fflush(w->csv_file);
    w->done = true;
    return NULL;
}

/* ======== SHM ======== */

/* Non-blocking: returns a live region if the producer (SHM /motor_ctrl) is
   already up, otherwise NULL without waiting. Polled from the main loop so a
   "start" issued before the producer is running waits until it appears. */
static shm_state_t *shm_try_connect(void)
{
    shm_state_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;

    const shm_region_t *r = (const shm_region_t *)MAP_FAILED;
    int fd = shm_open(MOTOR_SHM_NAME, O_RDONLY, 0);
    if (fd != -1) {
        r = (const shm_region_t *)mmap(NULL, sizeof(shm_region_t),
                                       PROT_READ, MAP_SHARED, fd, 0);
        close(fd);
    }
    if (r == MAP_FAILED || !motor_shm_region_valid(r)) {
        if (r != MAP_FAILED) munmap((void *)r, sizeof(shm_region_t));
        free(s);
        return NULL;
    }
    s->region = r;
    return s;
}

static void shm_disconnect(shm_state_t *s)
{
    if (!s) return;
    munmap((void *)s->region, sizeof(shm_region_t));
    free(s);
}

static size_t shm_poll(shm_state_t *s, shm_block_t *out, size_t max_blocks)
{
    const shm_block_ring_t *ring = &s->region->ring;
    uint64_t wp = motor_ring_write_pos(ring);
    uint32_t depth = ring->depth;

    if (wp - s->read_pos > depth) {
        uint64_t lapped = (wp - s->read_pos) - depth;
        s->dropped_blocks += lapped;
        s->read_pos = wp - depth;
    }

    size_t written = 0;
    while (s->read_pos < wp && written < max_blocks) {
        if (motor_ring_read_slot(ring, s->read_pos, &out[written])) {
            written++;
        } else {
            s->dropped_blocks++;
        }
        s->read_pos++;
    }
    return written;
}

/* ======== CSV file helpers ======== */

static FILE *open_csv(const char *path)
{
    FILE *f = fopen(path, "w");
    if (!f) { perror("fopen"); return NULL; }
    fprintf(f, "timestamp,Current_0,Current_1,Current_2,Speed_volt_cmd,"
               "Volt_0,Volt_1,Volt_2,DC_bus_volt,"
               "vib_x,vib_y,vib_z,rpm\n");
    return f;
}

static void generate_filename(char *buf, size_t bufsz)
{
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    snprintf(buf, bufsz, "%s/motor_%04d%02d%02d_%02d%02d%02d.csv",
             g_save_dir,
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec);
}

/* ======== Cleanup ======== */

static void on_signal(int signo)
{
    (void)signo;
    g_running = false;
}

static void recorder_cleanup(void)
{
    if (g_csv_file) { fflush(g_csv_file); fclose(g_csv_file); g_csv_file = NULL; }
    if (g_queue) { queue_free(g_queue); g_queue = NULL; }
    if (g_shm) { shm_disconnect(g_shm); g_shm = NULL; }
    if (g_rd) { free(g_rd); g_rd = NULL; }
    if (g_wr) { free(g_wr); g_wr = NULL; }
    g_rec_rows = 0;
}

static void finish_start_recording(const char *filename, uint64_t duration_sec)
{
    snprintf(g_csv_filename, sizeof(g_csv_filename), "%s", filename);

    g_csv_file = open_csv(filename);
    if (!g_csv_file) {
        fprintf(stderr, "[cmd] Failed to open CSV file: %s\n", filename);
        shm_disconnect(g_shm); g_shm = NULL;
        g_csv_filename[0] = '\0';
        return;
    }

    g_queue = queue_alloc(18);
    if (!g_queue) {
        fclose(g_csv_file); g_csv_file = NULL;
        shm_disconnect(g_shm); g_shm = NULL;
        g_csv_filename[0] = '\0';
        fprintf(stderr, "[cmd] Failed to allocate queue\n");
        return;
    }

    uint64_t wp = motor_ring_write_pos(&g_shm->region->ring);
    uint32_t depth = g_shm->region->ring.depth;
    g_shm->read_pos = (wp >= depth) ? (wp - depth) : 0;
    g_shm->dropped_blocks = 0;

    g_rd = calloc(1, sizeof(reader_t));
    g_wr = calloc(1, sizeof(writer_t));
    if (!g_rd || !g_wr) {
        recorder_cleanup();
        fprintf(stderr, "[cmd] Failed to allocate threads\n");
        return;
    }

    g_rd->shm = g_shm;
    g_rd->queue = g_queue;
    g_rd->active = true;
    g_rd->done = false;
    g_wr->queue = g_queue;
    g_wr->csv_file = g_csv_file;
    g_wr->active = true;
    g_wr->done = false;
    g_wr->rec_rows_ptr = &g_rec_rows;

    g_rec_rows = 0;
    g_recording = true;

    pthread_create(&g_rd_thr, NULL, reader_thread, g_rd);
    pthread_create(&g_wr_thr, NULL, writer_thread, g_wr);

    fprintf(stderr, "[cmd] Recording to %s (PID: %d)\n", filename, getpid());
    mqtt_client_publish_status(g_mqtt, REC_STATE_RECORDING, filename);

    if (duration_sec > 0) {
        g_auto_stop_time = now_ms() + duration_sec * 1000ULL;
        g_auto_stop_active = true;
        fprintf(stderr, "[cmd] Auto-stop at %llu ms (in %llu sec)\n",
                (unsigned long long)g_auto_stop_time,
                (unsigned long long)duration_sec);
    }
}

/* Returns 1 if recording started now, 0 if waiting for the producer. */
static int start_recording_or_wait(const char *filename, uint64_t duration_sec)
{
    if (g_waiting_for_producer) {
        snprintf(g_pending_filename, sizeof(g_pending_filename), "%s", filename);
        g_pending_duration = duration_sec;
        fprintf(stderr, "[cmd] Still waiting for data producer; updated pending start\n");
        return 0;
    }

    g_shm = shm_try_connect();
    if (!g_shm) {
        snprintf(g_pending_filename, sizeof(g_pending_filename), "%s", filename);
        g_pending_duration = duration_sec;
        g_waiting_for_producer = true;
        fprintf(stderr, "[cmd] Data producer not running (no /motor_ctrl); waiting for it...\n");
        mqtt_client_publish_status(g_mqtt, REC_STATE_IDLE,
                                   "waiting for data producer (no data)");
        return 0;
    }

    finish_start_recording(filename, duration_sec);
    return 1;
}

/* ======== Stop recording (extracted for reuse) ======== */

static void stop_recording(void)
{
    if (!g_recording) return;

    fprintf(stderr, "[cmd] Stopping recording...\n");

    g_recording = false;
    g_auto_stop_active = false;

    pthread_join(g_rd_thr, NULL);
    pthread_join(g_wr_thr, NULL);

    g_stop_span_us = 0;
    g_stop_total_rows = g_rec_rows;
    g_stop_drops = 0;
    g_stop_block_drops = 0;
    g_stop_stalled_ms = 0;

    if (g_rd) {
        if (g_rd->first_seen && g_rd->last_ts > g_rd->first_ts)
            g_stop_span_us = g_rd->last_ts - g_rd->first_ts;
        g_stop_drops = g_rd->dropped;
        g_stop_stalled_ms = g_rd->stalled_ms;
    }
    if (g_shm) {
        g_stop_block_drops = g_shm->dropped_blocks;
    }

    g_has_stop_data = true;
    recorder_cleanup();

    fprintf(stderr, "[cmd] Recording stopped. Rows: %llu, Span: %llu us, Drops: %llu, BlockDrops: %llu, Stalled: %llu ms, File: %s\n",
            (unsigned long long)g_stop_total_rows,
            (unsigned long long)g_stop_span_us,
            (unsigned long long)g_stop_drops,
            (unsigned long long)g_stop_block_drops,
            (unsigned long long)g_stop_stalled_ms,
            g_csv_filename);

    mqtt_client_publish_stop_metadata(g_mqtt, g_csv_filename, "",
                                      g_stop_span_us, g_stop_total_rows,
                                      g_stop_drops, g_stop_block_drops,
                                      g_stop_stalled_ms);
}

static void send_file_chunks(const char *filepath)
{
    FILE *f = fopen(filepath, "rb");
    if (!f) {
        fprintf(stderr, "[cmd] Failed to open file for reading: %s\n", filepath);
        return;
    }

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    char chunk_data[1024];
    int chunk_idx = 0;

    while (!feof(f) && !ferror(f)) {
        size_t bytes_read = fread(chunk_data, 1, sizeof(chunk_data), f);
        if (bytes_read > 0) {
            int total_chunks = (file_size + sizeof(chunk_data) - 1) / sizeof(chunk_data);
            mqtt_client_publish_chunk(g_mqtt, chunk_idx, total_chunks,
                                     chunk_data, bytes_read);
            chunk_idx++;
            if (chunk_idx % 50 == 0) {
                struct timespec ts = {0, 5 * 1000 * 1000L};
                nanosleep(&ts, NULL);
            }
        }
    }

    fclose(f);
    fprintf(stderr, "[cmd] Download complete. Chunks sent: %d\n", chunk_idx);
}

static void handle_list_command(void)
{
    DIR *d = opendir(g_save_dir);
    if (!d) {
        fprintf(stderr, "[cmd] Failed to open dir %s\n", g_save_dir);
        mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
            "{\"state\":\"file_list\",\"files\":[]}");
        return;
    }

    struct dirent *entry;
    char json[4096];
    size_t pos = (size_t)snprintf(json, sizeof(json),
                                  "{\"state\":\"file_list\",\"files\":[");
    bool first = true;

    /*
     * Each entry is built whole and only then committed.
     *
     * The separator used to be written before the entry was known to fit, and
     * with two independent size checks that disagreed. So a directory big
     * enough to fill this buffer produced either "},{" with the comma dropped
     * and no separator at all, or a trailing "," right before the "]" -- both
     * of which are invalid JSON, and the GUI drops a list it cannot parse
     * rather than showing the files that did fit. Reserve 3 bytes throughout
     * for the closing "]}" and the terminator, so closing it can never fail.
     */
    while ((entry = readdir(d)) != NULL) {
        if (!has_csv_suffix(entry->d_name))
            continue;

        char fullpath[1024];
        if (resolve_in_save_dir(entry->d_name, fullpath, sizeof(fullpath)) != 0)
            continue;                      /* readdir gave us something odd */

        struct stat st;
        long fsize = 0;
        if (stat(fullpath, &st) == 0)
            fsize = (long)st.st_size;

        char esc[600];
        json_escape(entry->d_name, esc, sizeof(esc));

        char item[768];
        int n = snprintf(item, sizeof(item),
                         "%s{\"name\":\"%s\",\"size\":%ld}",
                         first ? "" : ",", esc, fsize);
        if (n < 0 || (size_t)n >= sizeof(item))
            continue;
        if (pos + (size_t)n + 3 > sizeof(json))
            break;                         /* no room: stop cleanly, stay valid */

        memcpy(json + pos, item, (size_t)n);
        pos += (size_t)n;
        first = false;
    }
    closedir(d);

    memcpy(json + pos, "]}", 3);

    fprintf(stderr, "[cmd] File list response\n");
    mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC, json);
}

static void delete_reply(const char *filename, bool ok)
{
    char esc[600];
    json_escape(filename, esc, sizeof(esc));

    char resp[768];
    snprintf(resp, sizeof(resp),
             "{\"state\":\"delete_result\",\"file\":\"%s\",\"success\":%s}",
             esc, ok ? "true" : "false");
    mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC, resp);
}

static void handle_delete_command(const char *filename)
{
    char fullpath[1024];

    /* Refused before it reaches the filesystem: this used to build the path by
       pasting the name into "%s/%s", so `delete ../../<anything>` removed
       whatever it resolved to. */
    if (resolve_in_save_dir(filename, fullpath, sizeof(fullpath)) != 0) {
        fprintf(stderr, "[cmd] Rejected delete of '%s' (not a plain name in %s)\n",
                filename, g_save_dir);
        delete_reply(filename, false);
        return;
    }

    /* Only recordings. Nothing else in the save dir is this command's business,
       and the GUI never asks for anything else. */
    if (!has_csv_suffix(filename)) {
        fprintf(stderr, "[cmd] Rejected delete of '%s' (not a .csv)\n", filename);
        delete_reply(filename, false);
        return;
    }

    if (access(fullpath, F_OK) != 0) {
        fprintf(stderr, "[cmd] File not found for delete: %s\n", fullpath);
        delete_reply(filename, false);
        return;
    }

    if (remove(fullpath) == 0) {
        fprintf(stderr, "[cmd] Deleted: %s\n", fullpath);
        delete_reply(filename, true);
    } else {
        fprintf(stderr, "[cmd] Failed to delete: %s\n", fullpath);
        delete_reply(filename, false);
    }
}

/* ======== MQTT command handler ======== */

static void handle_command(mqtt_client_t *m, const char *cmd)
{
    g_mqtt = m;

    /* While we're still waiting for the data producer (SHM /motor_ctrl), any
       command other than "start" is answered with a producer-off notice. */
    if (g_waiting_for_producer && strncmp(cmd, "start", 5) != 0) {
        fprintf(stderr, "[cmd] Command '%s' ignored: data producer is off\n", cmd);
        mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
            "{\"state\":\"error\",\"msg\":\"producer is off (no data)\"}");
        return;
    }

    if (strncmp(cmd, "start", 5) == 0) {
        if (g_recording) {
            fprintf(stderr, "[cmd] Already recording\n");
            return;
        }

        const char *arg = cmd + 5;
        while (*arg == ' ') arg++;

        char filename[1024];
        uint64_t duration_sec = 0;

        if (*arg == '\0') {
            generate_filename(filename, sizeof(filename));
        } else {
            /*
             * %199s, not %255s. The buffer is 200 bytes and the conversion was
             * allowed to write 255 characters plus a terminator into it -- a
             * 56-byte stack overflow reachable by publishing a long enough
             * `start <name>` to the broker. The width and the buffer have to
             * be stated together or they drift; they are one line apart now.
             */
            char name_buf[200];
            int n = 0;
            if (sscanf(arg, "%199s%n", name_buf, &n) >= 1) {
                if (name_buf[0] >= '0' && name_buf[0] <= '9') {
                    duration_sec = strtoull(name_buf, NULL, 10);
                    generate_filename(filename, sizeof(filename));
                } else {
                    /* The name becomes "<save_dir>/<name>.csv", so a name
                       carrying a separator wrote the recording outside the save
                       directory -- the same hole as delete and download, just
                       creating instead of destroying. */
                    if (strchr(name_buf, '/') || strchr(name_buf, '\\')
                        || name_buf[0] == '.') {
                        fprintf(stderr, "[cmd] Rejected start name '%s' "
                                        "(must be a plain name)\n", name_buf);
                        mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                            "{\"state\":\"error\",\"msg\":\"invalid recording name\"}");
                        return;
                    }
                    snprintf(filename, sizeof(filename), "%s/%s.csv", g_save_dir, name_buf);
                    arg += n;
                    while (*arg == ' ') arg++;
                    if (*arg >= '0' && *arg <= '9') {
                        duration_sec = strtoull(arg, NULL, 10);
                    }
                }
            } else {
                generate_filename(filename, sizeof(filename));
            }
        }

        fprintf(stderr, "[cmd] Starting recording... file=%s dur=%llu\n",
                filename, (unsigned long long)duration_sec);

        start_recording_or_wait(filename, duration_sec);
    }
    else if (strcmp(cmd, "stop") == 0) {
        stop_recording();
    }
    else if (strncmp(cmd, "upload", 6) == 0) {
        const char *arg = cmd + 6;
        while (*arg == ' ') arg++;

        // Prevent multiple concurrent uploads
        if (g_uploading) {
            fprintf(stderr, "[cmd] Upload already in progress, ignoring new upload command\n");
            mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                "{\"state\":\"error\",\"msg\":\"Upload already in progress\"}");
            return;
        }

        char upload_path[1024] = "";
        if (*arg != '\0') {
            if (strchr(arg, '/'))
                snprintf(upload_path, sizeof(upload_path), "%s", arg);
            else
                snprintf(upload_path, sizeof(upload_path), "%s/%s", g_save_dir, arg);
        } else if (g_csv_filename[0] != '\0') {
            snprintf(upload_path, sizeof(upload_path), "%s", g_csv_filename);
        } else {
            fprintf(stderr, "[cmd] No file to upload\n");
            mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                "{\"state\":\"error\",\"msg\":\"No file to upload\"}");
            return;
        }

        if (access(upload_path, F_OK) != 0) {
            fprintf(stderr, "[cmd] File not found: %s\n", upload_path);
            mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                "{\"state\":\"error\",\"msg\":\"File not found\"}");
            return;
        }

        struct stat st;
        long long local_size = 0;
        if (stat(upload_path, &st) == 0)
            local_size = st.st_size;

        const char *fname = strrchr(upload_path, '/');
        fname = fname ? fname + 1 : upload_path;

        char remote_dir[2048];
        snprintf(remote_dir, sizeof(remote_dir),
                "/home/maxmaster/uploads/%s", fname);

        char remote_user[] = "maxmaster@139.185.38.211";

        /*
         * The file is streamed through ssh rather than handed to scp, so that
         * progress comes from bytes this process has actually written.
         *
         * What was here before could not report progress at all, for three
         * separate reasons stacked on top of each other:
         *
         *   - scp ran in a forked child and the parent watched it with
         *     waitpid(WNOHANG) in a loop whose only other statement was
         *     mqtt_client_loop(), which is a no-op on this build (the network
         *     runs on mosquitto's own thread). So the loop had no sleep in it
         *     at all: it spun at 100% CPU on a guest with ONE vCPU, competing
         *     with the scp it was waiting for.
         *
         *   - progress was sampled by ssh-ing to the server every few seconds
         *     to stat the partial file. Each of those is a fresh ssh handshake
         *     from inside a guest where a handshake alone measures ~5-10s, and
         *     popen() blocks for all of it -- so the sampler cost more than the
         *     interval it was sampling on.
         *
         *   - and it never ran anyway: the whole block was behind a flag that
         *     was initialised to 0 and never assigned.
         *
         * Writing the bytes ourselves removes all three. The percentage is
         * exact, it costs nothing to compute, and there is no second connection
         * to the server. `cat > file` is the same idiom the hypervisor manager
         * uses for its file pushes.
         */
        char ssh_put_cmd[4096];
        int put_n = snprintf(ssh_put_cmd, sizeof(ssh_put_cmd),
                "ssh -o ConnectTimeout=30 -o StrictHostKeyChecking=no "
                "-o ServerAliveInterval=10 -o ServerAliveCountMax=3 "
                "-i /.ssh/id_ed25519 %s \"cat > '%s'\"",
                remote_user, remote_dir);
        if (put_n < 0 || (size_t)put_n >= sizeof(ssh_put_cmd)) {
            mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                "{\"state\":\"error\",\"msg\":\"upload path too long\"}");
            return;
        }

        fprintf(stderr, "[cmd] Uploading %s (%lld bytes) ...\n",
                upload_path, local_size);
        fflush(stderr);

        g_uploading = true;

        FILE *src = fopen(upload_path, "rb");
        if (!src) {
            fprintf(stderr, "[cmd] cannot read %s\n", upload_path);
            mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                "{\"state\":\"error\",\"msg\":\"cannot read local file\"}");
            g_uploading = false;
            return;
        }

        FILE *sink = popen(ssh_put_cmd, "w");
        if (!sink) {
            fclose(src);
            fprintf(stderr, "[cmd] cannot start ssh for upload\n");
            mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                "{\"state\":\"error\",\"msg\":\"cannot start upload\"}");
            g_uploading = false;
            return;
        }

        mqtt_client_publish_upload_progress(g_mqtt, 0);

        char xfer[64 * 1024];
        long long sent = 0;
        int last_pct = 0;
        int write_failed = 0;
        size_t got;

        while ((got = fread(xfer, 1, sizeof(xfer), src)) > 0) {
            if (fwrite(xfer, 1, got, sink) != got) { write_failed = 1; break; }
            sent += (long long)got;

            if (local_size > 0) {
                int pct = (int)(sent * 100 / local_size);
                /* 99 is the ceiling until the transfer is known to have
                   succeeded: the last byte being written is not the same as
                   the server having kept the file. */
                if (pct > 99) pct = 99;
                if (pct > last_pct) {
                    last_pct = pct;
                    mqtt_client_publish_upload_progress(g_mqtt, pct);
                }
            }
        }
        if (ferror(src)) write_failed = 1;

        fclose(src);
        pclose(sink);   /* QNX pclose() is unreliable -- verified below instead */

        /*
         * One remote stat, after the fact, to decide success.
         *
         * pclose() on QNX reports -1 even for a child that exited 0, so it
         * cannot be the test. Comparing the size the server ended up with
         * against the size sent is a stronger check than an exit code anyway:
         * it catches a connection dropped mid-write, which is the failure that
         * actually happens here.
         */
        char ssh_stat_cmd[4096];
        snprintf(ssh_stat_cmd, sizeof(ssh_stat_cmd),
                "ssh -i /.ssh/id_ed25519 -o StrictHostKeyChecking=no "
                "-o ConnectTimeout=15 %s stat -c %%s '%s' 2>/dev/null",
                remote_user, remote_dir);

        long long remote_size = -1;
        FILE *sp = popen(ssh_stat_cmd, "r");
        if (sp) {
            char stat_buf[64];
            if (fgets(stat_buf, sizeof(stat_buf), sp))
                remote_size = atoll(stat_buf);
            pclose(sp);
        }

        int ok = !write_failed && remote_size == local_size;

        if (!ok) {
            fprintf(stderr, "[cmd] upload failed (sent=%lld of %lld, server has %lld)\n",
                    sent, local_size, remote_size);
            mqtt_client_publish_raw(g_mqtt, STATUS_TOPIC,
                "{\"state\":\"error\",\"msg\":\"upload failed or incomplete\"}");
            g_uploading = false;
            return;
        }

        fprintf(stderr, "[cmd] SCP done.\n");

        char remote_path[4096] = "";
        snprintf(remote_path, sizeof(remote_path),
                 "%s:%s", remote_user, remote_dir);
        mqtt_client_publish_upload_progress(g_mqtt, 100);

        mqtt_client_publish_stop_metadata(g_mqtt, upload_path, remote_path,
                                          g_stop_span_us, g_stop_total_rows,
                                          g_stop_drops, g_stop_block_drops,
                                          g_stop_stalled_ms);
        g_uploading = false;
    }
    else if (strcmp(cmd, "list") == 0) {
        handle_list_command();
    }
    else if (strncmp(cmd, "download", 8) == 0) {
        const char *arg = cmd + 8;
        while (*arg == ' ') arg++;

        /* No argument means "the recording just made". g_csv_filename holds a
           full path (generate_filename builds one), so reduce it to the plain
           name the containment check below expects -- it is in g_save_dir by
           construction, which is exactly what that check is enforcing. */
        if (*arg == '\0' && g_csv_filename[0] != '\0') {
            const char *slash = strrchr(g_csv_filename, '/');
            arg = slash ? slash + 1 : g_csv_filename;
        }

        if (*arg == '\0') {
            fprintf(stderr, "[cmd] No filename specified for download\n");
            return;
        }

        /*
         * The old form of this was, verbatim:
         *
         *     if (strchr(arg, '/')) snprintf(filepath, ..., "%s", arg);
         *
         * -- a '/' anywhere in the argument meant "treat it as an absolute
         * path and read it". So `download /etc/shadow` streamed any readable
         * file on the guest out over MQTT, to anyone able to publish to a
         * broker whose credentials are compiled into this repository. The
         * default local save name still works; a path does not.
         */
        char filepath[1024];
        if (resolve_in_save_dir(arg, filepath, sizeof(filepath)) != 0) {
            fprintf(stderr, "[cmd] Rejected download of '%s' (not a plain name in %s)\n",
                    arg, g_save_dir);
            return;
        }

        if (access(filepath, F_OK) != 0) {
            fprintf(stderr, "[cmd] File not found: %s\n", filepath);
            return;
        }

        fprintf(stderr, "[cmd] Downloading %s...\n", filepath);
        send_file_chunks(filepath);
    }
    else if (strncmp(cmd, "delete", 6) == 0) {
        const char *arg = cmd + 6;
        while (*arg == ' ') arg++;
        if (*arg == '\0') {
            fprintf(stderr, "[cmd] No filename specified for delete\n");
            return;
        }
        handle_delete_command(arg);
    }
    else {
        fprintf(stderr, "[cmd] Unknown command: %s\n", cmd);
    }
}

/* ======== main ======== */

int main(int argc, char *argv[])
{
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            snprintf(g_save_dir, sizeof(g_save_dir), "%s", argv[++i]);
        } else if (argv[i][0] != '-') {
            snprintf(g_save_dir, sizeof(g_save_dir), "%s", argv[i]);
        } else {
            continue;
        }
        struct stat st;
        if (stat(g_save_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Save dir does not exist: %s — using %s\n",
                    g_save_dir, DEFAULT_DIR);
            snprintf(g_save_dir, sizeof(g_save_dir), "%s", DEFAULT_DIR);
        }
    }

    fprintf(stderr, "Motor Recorder — save dir: %s\n", g_save_dir);

    mqtt_client_t mqtt;
    if (mqtt_client_init(&mqtt, handle_command) != 0) {
        fprintf(stderr, "Failed to initialize MQTT client\n");
        return 1;
    }

    if (mqtt_client_connect(&mqtt) != 0) {
        fprintf(stderr, "Failed to start MQTT connection\n");
        mqtt_client_disconnect(&mqtt);
        return 1;
    }

    fprintf(stderr, "Motor Recorder (MQTT-controlled)\n");
    fprintf(stderr, "Press Ctrl-C to exit\n");

    while (g_running) {
        struct timespec ts = {0, 100 * 1000 * 1000L};
        nanosleep(&ts, NULL);
        mqtt_client_ensure_connected(&mqtt);
        if (g_waiting_for_producer && !g_recording) {
            g_shm = shm_try_connect();
            if (g_shm) {
                g_waiting_for_producer = false;
                fprintf(stderr, "[main] Data producer online; starting recording now\n");
                char name[1024];
                uint64_t dur = g_pending_duration;
                snprintf(name, sizeof(name), "%s", g_pending_filename);
                g_pending_filename[0] = '\0';
                finish_start_recording(name, dur);
            }
        }
        if (g_auto_stop_active && g_recording && now_ms() >= g_auto_stop_time) {
            fprintf(stderr, "[main] Auto-stop triggered\n");
            stop_recording();
        }
    }

    fprintf(stderr, "\nShutting down...\n");

    if (g_recording) {
        g_recording = false;
        pthread_join(g_rd_thr, NULL);
        pthread_join(g_wr_thr, NULL);
        recorder_cleanup();
    }

    mqtt_client_disconnect(&mqtt);

    fprintf(stderr, "Goodbye!\n");
    return 0;
}
