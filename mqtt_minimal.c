/*
 * mqtt_minimal.c
 * Minimal MQTT 3.1.1 client implementation using QNX sockets.
 * Supports CONNECT, CONNACK, PUBLISH, SUBSCRIBE, PINGREQ.
 */
#include "mqtt_minimal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

/* MQTT Protocol Constants */
#define MQTT_PROTOCOL_NAME "MQTT"
#define MQTT_PROTOCOL_VERSION 4
#define MQTT_CONNECT_FLAG_CLEAN 0x02
#define MQTT_CONNECT_FLAG_WILL 0x04
#define MQTT_CONNECT_FLAG_WILL_QOS 0x08
#define MQTT_CONNECT_FLAG_WILL_RETAIN 0x10
#define MQTT_CONNECT_FLAG_PASSWORD 0x40
#define MQTT_CONNECT_FLAG_USERNAME 0x80

/* MQTT Return Codes */
#define MQTT_CONNACK_ACCEPTED 0
#define MQTT_CONNACK_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION 1
#define MQTT_CONNACK_REFUSED_IDENTIFIER_REJECTED 2
#define MQTT_CONNACK_REFUSED_SERVER_UNAVAILABLE 3
#define MQTT_CONNACK_REFUSED_BAD_USERNAME_OR_PASSWORD 4
#define MQTT_CONNACK_REFUSED_NOT_AUTHORIZED 5

static int mqtt_read(mqtt_client_t *m, void *buf, size_t len)
{
    size_t total = 0;
    while (total < len) {
        int n = recv(m->sock, (char *)buf + total, len - total, 0);
        if (n <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            return -1;
        }
        total += n;
    }
    return total;
}

static int mqtt_write(mqtt_client_t *m, const void *buf, size_t len)
{
    size_t total = 0;
    while (total < len) {
        int n = send(m->sock, (char *)buf + total, len - total, 0);
        if (n <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            return -1;
        }
        total += n;
    }
    return total;
}

/* Calculate remaining length */
static uint32_t mqtt_calc_remaining_length(uint32_t len)
{
    uint32_t result = 0;
    uint32_t multiplier = 1;
    while (len > 0) {
        uint8_t digit = len % 128;
        len /= 128;
        if (len > 0) digit |= 0x80;
        result += digit * multiplier;
        multiplier *= 128;
    }
    return result;
}

/* Write remaining length */
static void mqtt_write_remaining_length(uint8_t *buf, size_t *pos, uint32_t len)
{
    do {
        uint8_t byte = len % 128;
        len /= 128;
        if (len > 0) byte |= 0x80;
        buf[(*pos)++] = byte;
    } while (len > 0);
}

/* Build CONNECT packet */
static int mqtt_build_connect(uint8_t *buf, size_t *len, const char *client_id,
                              const char *username, const char *password)
{
    size_t pos = 0;

    /* Packet type: 0x10 (CONNECT) */
    buf[pos++] = 0x10;

    /* Remaining length */
    uint32_t remaining = 4 + strlen(MQTT_PROTOCOL_NAME) + 1 + 1 + strlen(client_id);
    if (username) remaining += strlen(username) + 1;
    if (password) remaining += strlen(password) + 1;
    mqtt_write_remaining_length(buf, &pos, remaining);
    buf[pos++] = 0; /* Flags */
    buf[pos++] = MQTT_KEEPALIVE & 0xFF;
    buf[pos++] = (MQTT_KEEPALIVE >> 8) & 0xFF;

    /* Protocol name */
    buf[pos++] = strlen(MQTT_PROTOCOL_NAME);
    memcpy(buf + pos, MQTT_PROTOCOL_NAME, strlen(MQTT_PROTOCOL_NAME));
    pos += strlen(MQTT_PROTOCOL_NAME);

    /* Protocol version */
    buf[pos++] = MQTT_PROTOCOL_VERSION;

    /* Client ID */
    buf[pos++] = strlen(client_id);
    memcpy(buf + pos, client_id, strlen(client_id));
    pos += strlen(client_id);

    /* Clean session flag */
    buf[pos++] = MQTT_CONNECT_FLAG_CLEAN;

    if (username) {
        buf[pos++] = strlen(username);
        memcpy(buf + pos, username, strlen(username));
        pos += strlen(username);
    }

    if (password) {
        buf[pos++] = strlen(password);
        memcpy(buf + pos, password, strlen(password));
        pos += strlen(password);
    }

    *len = pos;
    return 0;
}

/* Build SUBSCRIBE packet */
static int mqtt_build_subscribe(uint8_t *buf, size_t *len, uint16_t msg_id, const char *topic)
{
    size_t pos = 0;
    uint32_t remaining = strlen(topic) + 4; /* msg_id (2) + topic_len (2) + qos (1) */

    buf[pos++] = 0x82; /* Packet type + flags */
    mqtt_write_remaining_length(buf, &pos, remaining);
    buf[pos++] = msg_id & 0xFF;
    buf[pos++] = (msg_id >> 8) & 0xFF;
    buf[pos++] = strlen(topic);
    memcpy(buf + pos, topic, strlen(topic));
    pos += strlen(topic);
    buf[pos++] = 0; /* QoS 0 */

    *len = pos;
    return 0;
}

/* Build PUBLISH packet */
static int mqtt_build_publish(uint8_t *buf, size_t *len, const char *topic,
                              const void *payload, size_t payload_len)
{
    size_t pos = 0;
    uint32_t remaining = strlen(topic) + payload_len + 2; /* topic_len (2) */

    buf[pos++] = 0x30; /* Packet type + flags (QoS 0) */
    mqtt_write_remaining_length(buf, &pos, remaining);
    buf[pos++] = strlen(topic) & 0xFF;
    buf[pos++] = (strlen(topic) >> 8) & 0xFF;
    memcpy(buf + pos, topic, strlen(topic));
    pos += strlen(topic);
    memcpy(buf + pos, payload, payload_len);
    pos += payload_len;

    *len = pos;
    return 0;
}

/* Build PINGREQ packet */
static int mqtt_build_pingreq(uint8_t *buf, size_t *len)
{
    buf[0] = 0xC0;
    buf[1] = 0;
    *len = 2;
    return 0;
}

/* Parse variable header from received packet */
static int mqtt_parse_variable_header(uint8_t *buf, size_t len,
                                      uint8_t *packet_type, uint8_t *packet_flags,
                                      uint32_t *remaining_length)
{
    if (len < 2) return -1;

    *packet_type = buf[0] >> 4;
    *packet_flags = buf[0] & 0x0F;

    uint32_t ml = 0;
    uint32_t multiplier = 1;
    size_t pos = 1;

    while (pos < len) {
        uint8_t byte = buf[pos++];
        ml += (byte & 0x7F) * multiplier;
        multiplier *= 128;
        if (!(byte & 0x80)) break;
    }

    *remaining_length = ml;
    return 0;
}

/* Handle received data */
static int mqtt_handle_data(mqtt_client_t *m, uint8_t *buf, size_t len)
{
    size_t pos = 0;

    while (pos < len) {
        uint8_t packet_type;
        uint8_t packet_flags;
        uint32_t remaining_length;

        if (mqtt_parse_variable_header(buf + pos, len - pos, &packet_type, &packet_flags, &remaining_length) < 0) {
            return -1;
        }

        pos += 2; /* Skip fixed header */

        switch (packet_type) {
            case 0x20: /* CONNACK */
                if (remaining_length >= 2) {
                    uint8_t session_present = buf[pos];
                    uint8_t return_code = buf[pos + 1];

                    printf("[MQTT] Connected! Return code: %d\n", return_code);

                    if (return_code == MQTT_CONNACK_ACCEPTED) {
                        m->state = MQTT_STATE_CONNECTED;
                    } else {
                        m->state = MQTT_STATE_ERROR;
                        printf("[MQTT] Connection rejected: %d\n", return_code);
                    }
                    return 0;
                }
                break;

            case 0x32: /* PUBACK */
                /* QoS 1 acknowledgment - not handled for simplicity */
                break;

            case 0x40: /* PUBREC */
                break;

            case 0x50: /* PUBREL */
                break;

            case 0x52: /* PUBCOMP */
                break;

            case 0x80: /* PUBSUB */
                break;

            case 0xA0: /* PUBCOMP */
                break;

            case 0xB0: /* PUBREL */
                break;

            case 0xD0: /* PUBREC */
                break;

            case 0x91: /* PUBREL */
                break;

            case 0xA2: /* PUBCOMP */
                break;

            case 0xB2: /* PUBREL */
                break;

            case 0xD2: /* PUBREC */
                break;

            case 0xE0: /* PINGRESP */
                m->ping_timer = 0;
                break;
        }

        pos += remaining_length;
    }

    return 0;
}

int mqtt_minimal_init(mqtt_client_t *m, const char *client_id, const char *subscribe_topic, mqtt_callback_t cb)
{
    memset(m, 0, sizeof(*m));
    m->state = MQTT_STATE_DISCONNECTED;
    m->callback = cb;
    m->sock = -1;

    if (subscribe_topic) {
        strncpy(m->topic_sub, subscribe_topic, sizeof(m->topic_sub) - 1);
    }
    strncpy(m->topic_publish, "guest/rpi5guest1/status", sizeof(m->topic_publish) - 1);

    m->ping_timer = 0;
    m->reconnect_timer = 0;

    return 0;
}

int mqtt_minimal_connect(mqtt_client_t *m, const char *username, const char *password)
{
    struct sockaddr_in addr;
    int flags;

    m->sock = socket(AF_INET, SOCK_STREAM, 0);
    if (m->sock < 0) {
        perror("socket");
        return -1;
    }

    flags = fcntl(m->sock, F_GETFL, 0);
    fcntl(m->sock, F_SETFL, flags | O_NONBLOCK);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(MQTT_PORT);
    if (inet_pton(AF_INET, MQTT_BROKER, &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(m->sock);
        m->sock = -1;
        return -1;
    }

    if (connect(m->sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
            perror("connect");
            close(m->sock);
            m->sock = -1;
            return -1;
        }
    }

    /* Send CONNECT */
    uint8_t conn_buf[256];
    size_t conn_len;
    if (mqtt_build_connect(conn_buf, &conn_len, MQTT_CLIENT_ID, username, password) < 0) {
        close(m->sock);
        m->sock = -1;
        return -1;
    }

    if (mqtt_write(m, conn_buf, conn_len) < 0) {
        perror("mqtt_write");
        close(m->sock);
        m->sock = -1;
        return -1;
    }

    m->state = MQTT_STATE_CONNECTING;

    return 0;
}

void mqtt_minimal_disconnect(mqtt_client_t *m)
{
    if (m->sock >= 0) {
        close(m->sock);
        m->sock = -1;
    }
    m->state = MQTT_STATE_DISCONNECTED;
}

int mqtt_minimal_poll(mqtt_client_t *m, int timeout_ms)
{
    if (m->state != MQTT_STATE_CONNECTED) {
        return 0;
    }

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(m->sock, &read_fds);

    int ret = select(m->sock + 1, &read_fds, NULL, NULL, &tv);
    if (ret < 0) {
        if (errno == EINTR) return 0;
        perror("select");
        mqtt_minimal_disconnect(m);
        return -1;
    }

    if (ret > 0 && FD_ISSET(m->sock, &read_fds)) {
        uint8_t recv_buf[1024];
        int n = recv(m->sock, recv_buf, sizeof(recv_buf), 0);
        if (n <= 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("recv");
                mqtt_minimal_disconnect(m);
                return -1;
            }
        } else {
            mqtt_handle_data(m, recv_buf, n);
        }
    }

    return 0;
}

int mqtt_minimal_publish(mqtt_client_t *m, const char *topic, const void *payload, size_t len)
{
    uint8_t pub_buf[1024];
    size_t pub_len;

    if (mqtt_build_publish(pub_buf, &pub_len, topic, payload, len) < 0) {
        return -1;
    }

    if (mqtt_write(m, pub_buf, pub_len) < 0) {
        perror("mqtt_write");
        return -1;
    }

    return 0;
}

int mqtt_parse_topic(const char *str, char *topic, size_t topic_max)
{
    if (!str || !topic) return -1;

    size_t len = 0;
    while (*str && *str != ' ' && len < topic_max - 1) {
        topic[len++] = *str++;
    }
    topic[len] = '\0';
    return len;
}

int mqtt_parse_command(const char *str, char *cmd, size_t cmd_max)
{
    if (!str || !cmd) return -1;

    size_t len = 0;
    while (*str && *str != ' ' && *str != '\n' && len < cmd_max - 1) {
        cmd[len++] = *str++;
    }
    cmd[len] = '\0';
    return len;
}
