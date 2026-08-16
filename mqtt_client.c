/*
 * mqtt_client.c
 * Implementation of MQTT client wrapper using libmosquitto.
 */
#include "mqtt_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* wall-clock milliseconds, for throttling reconnect attempts */
static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)(ts.tv_nsec / 1000000u);
}

static void cmd_callback(struct mosquitto *mosq, void *userdata,
                        const struct mosquitto_message *msg)
{
    (void)mosq;
    mqtt_client_t *m = (mqtt_client_t *)userdata;

    /*
     * Copied into a buffer we terminate ourselves, rather than handed straight
     * to the command parser.
     *
     * Two reasons, and the first is a remote crash. An MQTT message may have a
     * zero-length payload, and libmosquitto reports that as payload == NULL --
     * which went directly to handle_command(), whose very first act is
     * strncmp(cmd, "start", 5). Publishing an empty message to the command
     * topic was enough to segfault the recorder.
     *
     * Second, a payload is a byte buffer with a length, not a C string. This
     * build of libmosquitto does append a terminator as a convenience, but the
     * parser below runs strcmp/strncmp/sscanf over it, so depending on that is
     * depending on a courtesy rather than on the protocol.
     */
    char cmd[512];
    int n = msg->payloadlen;
    if (n < 0) n = 0;
    if (n >= (int)sizeof(cmd)) n = (int)sizeof(cmd) - 1;
    if (n > 0 && msg->payload)
        memcpy(cmd, msg->payload, (size_t)n);
    else
        n = 0;
    cmd[n] = '\0';

    printf("[MQTT] Received cmd: %s on topic %s\n",
           cmd, msg->topic ? msg->topic : "(none)");

    if (m->cmd_callback)
        m->cmd_callback(m, cmd);
}

static void on_connect(struct mosquitto *mosq, void *userdata, int rc)
{
    (void)mosq;
    mqtt_client_t *m = (mqtt_client_t *)userdata;

    if (rc == 0) {
        m->connected = true;
        m->connecting = false;
        m->connect_attempt_ms = 0;
        printf("[MQTT] Connected to broker\n");
        mosquitto_subscribe(m->mosq, NULL, CMD_TOPIC, 0);
        char status_buf[512];
        int len = snprintf(status_buf, sizeof(status_buf),
                          "{\"state\":\"%s\",\"msg\":\"%s\"}",
                          (m->state == REC_STATE_IDLE) ? "idle" :
                          (m->state == REC_STATE_RECORDING) ? "recording" : "stopped",
                          m->status_msg);
        if (len > 0 && len < (int)sizeof(status_buf))
            mosquitto_publish(m->mosq, NULL, STATUS_TOPIC, len, status_buf, 0, false);
    } else {
        m->connected = false;
        m->connecting = false;
        fprintf(stderr, "[MQTT] Connect failed (rc=%d, %s)\n", rc, mosquitto_strerror(rc));
    }
}

static void on_disconnect(struct mosquitto *mosq, void *userdata, int rc)
{
    (void)mosq;
    mqtt_client_t *m = (mqtt_client_t *)userdata;
    m->connected = false;
    m->connecting = false;
    fprintf(stderr, "[MQTT] Disconnected (rc=%d) — will retry\n", rc);
}

int mqtt_client_init(mqtt_client_t *m, cmd_callback_t cb)
{
    memset(m, 0, sizeof(*m));
    m->cmd_callback = cb;

    mosquitto_lib_init();

    m->mosq = mosquitto_new("motor_recorder", true, m);
    if (!m->mosq) {
        fprintf(stderr, "[MQTT] Failed to create mosquitto instance\n");
        return -1;
    }

    mosquitto_username_pw_set(m->mosq, MQTT_USER, MQTT_PASS);
    mosquitto_connect_callback_set(m->mosq, on_connect);
    mosquitto_disconnect_callback_set(m->mosq, on_disconnect);
    mosquitto_message_callback_set(m->mosq, cmd_callback);
    mosquitto_reconnect_delay_set(m->mosq, 1, 30, true);

    m->state = REC_STATE_IDLE;
    snprintf(m->status_msg, sizeof(m->status_msg), "idle");
    m->last_data_ts = 0;
    m->connected = false;
    m->connecting = false;
    m->connect_attempt_ms = 0;
    m->last_kick_ms = 0;
    m->reconnect_enabled = true;

    return 0;
}

int mqtt_client_connect(mqtt_client_t *m)
{
    /*
     * mosquitto_connect_async() is called here only to record the broker
     * host/port in the client. On this QNX libmosquitto build the async
     * connect path is broken: for any connection that does not complete
     * synchronously it reports "Socket is not connected" (ENOTCONN). We
     * therefore ignore its return value and let the network thread started
     * below do the actual connecting.
     *
     * mosquitto_loop_start() runs mosquitto_loop_forever() in the
     * background, which automatically reconnects with the delay/backoff set
     * by mosquitto_reconnect_delay_set() in mqtt_client_init() and uses the
     * working blocking mosquitto_reconnect() path. on_connect() re-subscribes
     * and re-publishes status once connected, so the recorder keeps retrying
     * in the background until the broker becomes reachable.
     */
    int rc = mosquitto_connect_async(m->mosq, MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Initial async connect: %s (ignoring, retrying in background)\n",
                mosquitto_strerror(rc));
    }

    rc = mosquitto_loop_start(m->mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Failed to start network thread: %s\n",
                mosquitto_strerror(rc));
        return -1;
    }

    printf("[MQTT] Connecting to %s:%d (background, retrying until connected)\n",
           MQTT_BROKER, MQTT_PORT);
    return 0;
}

/*
 * Polled from the recorder's main loop. Reliably reclaims the connection
 * whenever it drops:
 *  - Mosquitto's own loop thread reconnects on its own on most builds, but on
 *    this QNX build that path has already proven unreliable (the async connect
 *    bug above), so we kick it explicitly.
 *  - If an attempt is already in flight (connecting), we do nothing — but we
 *    also watchdog it: a syscall-level failure that never fires the connect/
 *    disconnect callbacks would otherwise leave connecting stuck forever, so
 *    after 15 s we force the flag clear and let the next poll re-kick.
 */
void mqtt_client_ensure_connected(mqtt_client_t *m)
{
    uint64_t now = now_ms();

    if (!m->mosq || m->connected || !m->reconnect_enabled)
        return;

    if (m->connecting) {
        if (m->connect_attempt_ms && now - m->connect_attempt_ms > 15000)
            m->connecting = false;
        else
            return;
    }

    /* Never kick more often than once per 2 s. */
    if (now - m->last_kick_ms < 2000)
        return;
    m->last_kick_ms = now;

    m->connecting = true;
    m->connect_attempt_ms = now;

    int rc = mosquitto_reconnect_async(m->mosq);
    if (rc == MOSQ_ERR_SUCCESS || rc == MOSQ_ERR_CONN_PENDING) {
        printf("[MQTT] Retrying connection to %s:%d\n", MQTT_BROKER, MQTT_PORT);
        return;
    }

    m->connecting = false;
    m->connect_attempt_ms = 0;
    if (rc != MOSQ_ERR_NO_CONN)
        fprintf(stderr, "[MQTT] Reconnect attempt failed: %s\n",
                mosquitto_strerror(rc));
}

int mqtt_client_subscribe(mqtt_client_t *m)
{
    if (mosquitto_subscribe(m->mosq, NULL, CMD_TOPIC, 0) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Failed to subscribe to %s\n", CMD_TOPIC);
        return -1;
    }

    printf("[MQTT] Subscribed to %s\n", CMD_TOPIC);
    return 0;
}

void mqtt_client_disconnect(mqtt_client_t *m)
{
    m->reconnect_enabled = false;
    m->connected = false;
    m->connecting = false;
    if (m->mosq) {
        mosquitto_loop_stop(m->mosq, true);
        mosquitto_disconnect(m->mosq);
        mosquitto_destroy(m->mosq);
        m->mosq = NULL;
    }
    mosquitto_lib_cleanup();
}

/*
 * Escape a string for a JSON double-quoted value. Always terminates.
 *
 * Messages published from here are filenames and error text, both of which can
 * contain a quote or a backslash -- `start <name>` takes the name straight from
 * the command. Pasted in raw they produced a payload the GUI's JSON.parse()
 * rejected, so the reply was dropped whole instead of being shown wrong.
 */
static void json_escape_str(const char *in, char *out, size_t out_sz)
{
    size_t j = 0;
    if (out_sz == 0) return;
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
}

void mqtt_client_publish_status(mqtt_client_t *m, rec_state_t state, const char *msg)
{
    if (!msg) msg = "";
    m->state = state;

    /*
     * "%s", msg -- not msg.
     *
     * This was snprintf(m->status_msg, sizeof(m->status_msg), msg), which hands
     * the caller's string to printf as the FORMAT. msg is a filename here, and
     * filenames come from `start <name>` over MQTT, so `start %s%s%s` walked
     * varargs that were never pushed and `start %n` was a write through one.
     * A format string is never the right place for data.
     */
    snprintf(m->status_msg, sizeof(m->status_msg), "%s", msg);

    char esc[300];
    json_escape_str(msg, esc, sizeof(esc));

    char status_buf[512];
    int len = snprintf(status_buf, sizeof(status_buf),
                      "{\"state\":\"%s\",\"msg\":\"%s\"}",
                      (state == REC_STATE_IDLE) ? "idle" :
                      (state == REC_STATE_RECORDING) ? "recording" : "stopped",
                      esc);

    /* (size_t)len, so a truncating snprintf -- which returns the length it
       WOULD have written -- cannot compare as "fits" against a signed int. */
    if (len > 0 && (size_t)len < sizeof(status_buf)) {
        mosquitto_publish(m->mosq, NULL, STATUS_TOPIC, len, status_buf, 0, false);
    }
}

size_t mqtt_client_format_row(char *buf, size_t buf_sz, uint64_t ts,
                              const motor_row_t *row)
{
    int len = snprintf(buf, buf_sz,
                      "%llu,%u,%u,%u,%u,%u,%u,%u,%u,%d,%d,%d,%u\n",
                      (unsigned long long)ts,
                      row->current[0], row->current[1], row->current[2], row->current[3],
                      row->current[4], row->current[5], row->current[6], row->current[7],
                      row->vib_x, row->vib_y, row->vib_z, row->rpm);

    if (len <= 0 || (size_t)len >= buf_sz) return 0;
    return (size_t)len;
}

/*
 * One message, one or more rows.
 *
 * The rows are newline-terminated and the GUI splits on the newline, so a block
 * of several is the same format as the single row this used to send -- a one-row
 * block is byte-for-byte what it was. That is what lets the sample rate go up
 * without the message rate following it: the live plot wants detail, the broker
 * is ~145ms away and wants few round trips, and packing decouples the two.
 */
void mqtt_client_publish_rows(mqtt_client_t *m, const char *payload, size_t len,
                              uint64_t ts)
{
    if (!payload || len == 0) return;
    mosquitto_publish(m->mosq, NULL, DATA_TOPIC, (int)len, payload, 0, false);
    m->last_data_ts = ts;
}

void mqtt_client_publish_row(mqtt_client_t *m, uint64_t ts, const motor_row_t *row)
{
    char row_buf[512];
    size_t len = mqtt_client_format_row(row_buf, sizeof(row_buf), ts, row);
    if (len) mqtt_client_publish_rows(m, row_buf, len, ts);
}

void mqtt_client_publish_chunk(mqtt_client_t *m, int chunk_idx, int total_chunks,
                               const char *chunk_data, size_t chunk_len)
{
    char chunk_buf[8192];
    int pos = snprintf(chunk_buf, sizeof(chunk_buf),
                      "{\"chunk\":%d,\"total\":%d,\"data\":\"",
                      chunk_idx, total_chunks);
    /*
     * -8, not -6.
     *
     * The widest escape below writes 6 bytes, so -6 kept the loop itself in
     * bounds -- but it could leave pos with only two bytes spare, too few for
     * the closing "\"}" and its terminator. Reserving the tail here means the
     * close below can be unconditional, which is what the bug underneath it
     * needed.
     */
    for (size_t i = 0; i < chunk_len && pos < (int)sizeof(chunk_buf) - 8; ++i) {
        unsigned char c = (unsigned char)chunk_data[i];
        if (c == '"' || c == '\\') {
            chunk_buf[pos++] = '\\';
            chunk_buf[pos++] = c;
        } else if (c == '\n') {
            chunk_buf[pos++] = '\\';
            chunk_buf[pos++] = 'n';
        } else if (c == '\r') {
            chunk_buf[pos++] = '\\';
            chunk_buf[pos++] = 'r';
        } else if (c == '\t') {
            chunk_buf[pos++] = '\\';
            chunk_buf[pos++] = 't';
        } else if (c < 0x20) {
            pos += snprintf(chunk_buf + pos, sizeof(chunk_buf) - pos,
                           "\\u%04x", c);
        } else {
            chunk_buf[pos++] = c;
        }
    }
    /*
     * Closing the object is unconditional now, because the length below is.
     *
     * It used to be conditional -- and when the condition failed it wrote only
     * a terminator, no "\"}" -- while `len = pos + 2` was computed either way.
     * So on that path the publish claimed two bytes that had never been
     * written: the payload was unterminated JSON, which the GUI drops, with two
     * bytes of whatever was on the stack appended to it. Rare, since it needs a
     * chunk that escapes to nearly 8 KB, but it leaked stack either way.
     *
     * The loop above reserves the room, so this always fits.
     */
    memcpy(chunk_buf + pos, "\"}", 3);
    int len = pos + 2;
    mosquitto_publish(m->mosq, NULL, DOWNLOAD_TOPIC, len, chunk_buf, 0, false);
}

void mqtt_client_publish_upload_progress(mqtt_client_t *m, int percent)
{
    if (percent < 0) percent = 0;
    if (percent > 100) percent = 100;

    char buf[256];
    int len = snprintf(buf, sizeof(buf),
                      "{\"state\":\"uploading\",\"progress\":%d}",
                      percent);

    if (len > 0 && (size_t)len < sizeof(buf)) {
        mosquitto_publish(m->mosq, NULL, STATUS_TOPIC, len, buf, 0, false);
    }
}

void mqtt_client_publish_stop_metadata(mqtt_client_t *m, const char *file,
                                        const char *url,
                                        uint64_t span_us, uint64_t rows,
                                        uint64_t drops, uint64_t block_drops,
                                        uint64_t stalled_ms)
{
    if (!file) file = "";
    if (!url)  url  = "";     /* *url below dereferenced this unchecked */

    m->state = REC_STATE_STOPPED;
    snprintf(m->status_msg, sizeof(m->status_msg), "%s", file);

    /* Both are escaped: `file` is a recording name, and those come from
       `start <name>` over MQTT, so a quote in one used to produce a payload the
       GUI could not parse -- losing the entire end-of-recording report,
       including the row and drop counts, rather than just the name. */
    char file_esc[600], url_esc[600];
    json_escape_str(file, file_esc, sizeof(file_esc));
    json_escape_str(url,  url_esc,  sizeof(url_esc));

    char buf[1536];
    int len = snprintf(buf, sizeof(buf),
                      "{"
                      "\"state\":\"stopped\","
                      "\"msg\":\"Recording complete\","
                      "\"file\":\"%s\","
                      "\"url\":\"%s\","
                      "\"span_us\":%llu,"
                      "\"rows\":%llu,"
                      "\"drops\":%llu,"
                      "\"block_drops\":%llu,"
                      "\"stalled_ms\":%llu"
                      "}",
                      file_esc, url_esc,
                      (unsigned long long)span_us,
                      (unsigned long long)rows,
                      (unsigned long long)drops,
                      (unsigned long long)block_drops,
                      (unsigned long long)stalled_ms);

    if (len > 0 && (size_t)len < sizeof(buf)) {
        mosquitto_publish(m->mosq, NULL, STATUS_TOPIC, len, buf, 0, false);
    }
}

void mqtt_client_publish_raw(mqtt_client_t *m, const char *topic, const char *payload)
{
    if (!m->mosq) return;
    size_t len = strlen(payload);
    mosquitto_publish(m->mosq, NULL, topic, len, payload, 0, false);
}

void mqtt_client_loop(mqtt_client_t *m, int timeout_ms)
{
    (void)m;
    (void)timeout_ms;
    /* Network handling runs in the background thread started by
       mosquitto_loop_start(); this is kept as a no-op for compatibility. */
}
