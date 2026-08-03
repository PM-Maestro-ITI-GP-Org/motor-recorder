/*
 * mqtt_client.c
 * Implementation of MQTT client wrapper using libmosquitto.
 */
#include "mqtt_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void cmd_callback(struct mosquitto *mosq, void *userdata,
                        const struct mosquitto_message *msg)
{
    mqtt_client_t *m = (mqtt_client_t *)userdata;
    const char *topic = msg->topic;
    const char *payload = (const char *)msg->payload;
    int payload_len = msg->payloadlen;

    printf("[MQTT] Received cmd: %.*s on topic %s\n", payload_len, payload, topic);

    if (m->cmd_callback) {
        m->cmd_callback(m, payload);
    }
}

static void on_connect(struct mosquitto *mosq, void *userdata, int rc)
{
    mqtt_client_t *m = (mqtt_client_t *)userdata;

    if (rc == 0) {
        m->connected = true;
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
        fprintf(stderr, "[MQTT] Connect failed (rc=%d, %s)\n", rc, mosquitto_strerror(rc));
    }
}

static void on_disconnect(struct mosquitto *mosq, void *userdata, int rc)
{
    (void)mosq;
    mqtt_client_t *m = (mqtt_client_t *)userdata;
    m->connected = false;
    fprintf(stderr, "[MQTT] Disconnected (rc=%d) — will retry\n", rc);
}

int mqtt_client_init(mqtt_client_t *m, cmd_callback_t cb)
{
    memset(m, 0, sizeof(*m));
    m->cmd_callback = cb;

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
    m->reconnect_enabled = true;

    return 0;
}

int mqtt_client_connect(mqtt_client_t *m)
{
    int rc = mosquitto_connect_async(m->mosq, MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Connection failed: %s\n", mosquitto_strerror(rc));
        return -1;
    }

    rc = mosquitto_loop_start(m->mosq);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Failed to start network thread: %s\n",
                mosquitto_strerror(rc));
        return -1;
    }

    printf("[MQTT] Connecting to %s:%d (async, will retry until connected)\n",
           MQTT_BROKER, MQTT_PORT);
    return 0;
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
    if (m->mosq) {
        mosquitto_loop_stop(m->mosq, true);
        mosquitto_disconnect(m->mosq);
        mosquitto_destroy(m->mosq);
        m->mosq = NULL;
    }
}

void mqtt_client_publish_status(mqtt_client_t *m, rec_state_t state, const char *msg)
{
    m->state = state;
    snprintf(m->status_msg, sizeof(m->status_msg), msg);

    char status_buf[512];
    int len = snprintf(status_buf, sizeof(status_buf),
                      "{\"state\":\"%s\",\"msg\":\"%s\"}", 
                      (state == REC_STATE_IDLE) ? "idle" :
                      (state == REC_STATE_RECORDING) ? "recording" : "stopped",
                      msg);

    if (len > 0 && len < sizeof(status_buf)) {
        mosquitto_publish(m->mosq, NULL, STATUS_TOPIC, len, status_buf, 0, false);
    }
}

void mqtt_client_publish_row(mqtt_client_t *m, uint64_t ts, const motor_row_t *row)
{
    char row_buf[512];
    int len = snprintf(row_buf, sizeof(row_buf),
                      "%llu,%u,%u,%u,%u,%u,%u,%u,%u,%d,%d,%d,%u\n",
                      (unsigned long long)ts,
                      row->current[0], row->current[1], row->current[2], row->current[3],
                      row->current[4], row->current[5], row->current[6], row->current[7],
                      row->vib_x, row->vib_y, row->vib_z, row->rpm);

    if (len > 0 && len < sizeof(row_buf)) {
        mosquitto_publish(m->mosq, NULL, DATA_TOPIC, len, row_buf, 0, false);
    }

    m->last_data_ts = ts;
}

void mqtt_client_publish_chunk(mqtt_client_t *m, int chunk_idx, int total_chunks,
                               const char *chunk_data, size_t chunk_len)
{
    char chunk_buf[8192];
    int pos = snprintf(chunk_buf, sizeof(chunk_buf),
                      "{\"chunk\":%d,\"total\":%d,\"data\":\"",
                      chunk_idx, total_chunks);
    for (size_t i = 0; i < chunk_len && pos < (int)sizeof(chunk_buf) - 6; ++i) {
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
    if (pos < (int)sizeof(chunk_buf) - 3)
        memcpy(chunk_buf + pos, "\"}", 3);
    else
        chunk_buf[pos] = '\0';
    int len = pos + 2;
    if (len > 0)
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

    if (len > 0 && len < sizeof(buf)) {
        mosquitto_publish(m->mosq, NULL, STATUS_TOPIC, len, buf, 0, false);
    }
}

void mqtt_client_publish_stop_metadata(mqtt_client_t *m, const char *file,
                                        const char *url,
                                        uint64_t span_us, uint64_t rows,
                                        uint64_t drops, uint64_t block_drops,
                                        uint64_t stalled_ms)
{
    m->state = REC_STATE_STOPPED;
    snprintf(m->status_msg, sizeof(m->status_msg), "%s", file);

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
                      file, *url ? url : "",
                      (unsigned long long)span_us,
                      (unsigned long long)rows,
                      (unsigned long long)drops,
                      (unsigned long long)block_drops,
                      (unsigned long long)stalled_ms);

    if (len > 0 && len < sizeof(buf)) {
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
