#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#include <mosquitto.h>
#include <stdint.h>
#include <stdbool.h>

#include "motor_shm.h"

#define MQTT_BROKER "139.185.38.211"
#define MQTT_PORT 1883
#define MQTT_USER "mqttuser"
#define MQTT_PASS "123456"
#define MQTT_KEEPALIVE 60

#define STATUS_TOPIC "guest/rpi5guest1/status"
#define CMD_TOPIC "guest/rpi5guest1/cmd"
#define DATA_TOPIC "guest/rpi5guest1/data"
#define DOWNLOAD_TOPIC "guest/rpi5guest1/download"

typedef enum {
    REC_STATE_IDLE,
    REC_STATE_RECORDING,
    REC_STATE_STOPPED,
    REC_STATE_DOWNLOADING
} rec_state_t;

typedef struct mqtt_client mqtt_client_t;

typedef void (*cmd_callback_t)(mqtt_client_t *m, const char *cmd);

struct mqtt_client {
    struct mosquitto *mosq;
    rec_state_t state;
    char status_msg[256];
    uint64_t last_data_ts;
    cmd_callback_t cmd_callback;
    volatile bool connected;
    volatile bool connecting;
    volatile bool reconnect_enabled;
    uint64_t connect_attempt_ms;
    uint64_t last_kick_ms;
};

int mqtt_client_init(mqtt_client_t *m, cmd_callback_t cb);
int mqtt_client_connect(mqtt_client_t *m);
int mqtt_client_subscribe(mqtt_client_t *m);
void mqtt_client_disconnect(mqtt_client_t *m);
/* Poll this from the main loop: kicks a reconnect whenever the client is not
   connected and no attempt is in flight. Safe to call repeatedly. */
void mqtt_client_ensure_connected(mqtt_client_t *m);
void mqtt_client_publish_status(mqtt_client_t *m, rec_state_t state, const char *msg);
void mqtt_client_publish_row(mqtt_client_t *m, uint64_t ts, const motor_row_t *row);

/* Format one data row, newline-terminated, in the wire format the GUI parses.
   Returns bytes written, 0 if it would not fit. Split out from
   publish_row() so several rows can be packed into one message. */
size_t mqtt_client_format_row(char *buf, size_t buf_sz, uint64_t ts,
                              const motor_row_t *row);

/* Publish an already-formatted block of one or more rows to the data topic.
   `ts` is the timestamp of the last row in the block. */
void mqtt_client_publish_rows(mqtt_client_t *m, const char *payload, size_t len,
                              uint64_t ts);
void mqtt_client_publish_chunk(mqtt_client_t *m, int chunk_idx, int total_chunks,
                               const char *chunk_data, size_t chunk_len);
void mqtt_client_publish_upload_progress(mqtt_client_t *m, int percent);
void mqtt_client_publish_stop_metadata(mqtt_client_t *m, const char *file,
                                        const char *url,
                                        uint64_t span_us, uint64_t rows,
                                        uint64_t drops, uint64_t block_drops,
                                        uint64_t stalled_ms);
void mqtt_client_publish_raw(mqtt_client_t *m, const char *topic, const char *payload);
void mqtt_client_loop(mqtt_client_t *m, int timeout_ms);

#endif
