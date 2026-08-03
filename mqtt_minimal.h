/*
 * mqtt_minimal.h
 * Minimal MQTT client implementation using QNX sockets (no external libraries).
 * Supports MQTT 3.1.1 protocol.
 */
#ifndef MQTT_MINIMAL_H
#define MQTT_MINIMAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#define MQTT_BROKER "139.185.38.211"
#define MQTT_PORT 1883
#define MQTT_CLIENT_ID "motor_recorder"
#define MQTT_KEEPALIVE 60

typedef enum {
    MQTT_STATE_DISCONNECTED,
    MQTT_STATE_CONNECTING,
    MQTT_STATE_CONNECTED,
    MQTT_STATE_ERROR
} mqtt_state_t;

typedef void (*mqtt_callback_t)(const char *topic, const void *payload, size_t len);

typedef struct {
    mqtt_state_t state;
    mqtt_callback_t callback;
    int sock;
    char topic_sub[256];
    char topic_publish[256];
    int ping_timer;
    int reconnect_timer;
} mqtt_client_t;

/* Initialize MQTT client */
int mqtt_minimal_init(mqtt_client_t *m, const char *client_id, const char *subscribe_topic, mqtt_callback_t cb);

/* Connect to broker */
int mqtt_minimal_connect(mqtt_client_t *m, const char *username, const char *password);

/* Disconnect */
void mqtt_minimal_disconnect(mqtt_client_t *m);

/* Process network events */
int mqtt_minimal_poll(mqtt_client_t *m, int timeout_ms);

/* Publish message */
int mqtt_minimal_publish(mqtt_client_t *m, const char *topic, const void *payload, size_t len);

/* Helper to parse topic from string */
int mqtt_parse_topic(const char *str, char *topic, size_t topic_max);

/* Helper to parse command from string */
int mqtt_parse_command(const char *str, char *cmd, size_t cmd_max);

#endif /* MQTT_MINIMAL_H */
