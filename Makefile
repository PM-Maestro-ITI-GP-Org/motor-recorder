QNX800_DIR := ../../qnx800
MQTT_LIBS := ../mqtt_libs/install_qnx
CJSON_LIBS := $(MQTT_LIBS)/cJSON

CFLAGS := -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -O2 \
	-I../motor_data_producer/QNX-SPI \
	-I$(MQTT_LIBS)/include \
	-I$(CJSON_LIBS)/include

LDFLAGS := -L$(MQTT_LIBS)/lib -L$(CJSON_LIBS)/lib \
	-lmosquitto -lcjson \
	-lsocket -lm

.PHONY: all qnx clean

all: qnx

qnx:
	@bash -c 'set -e; \
		. "$(QNX800_DIR)/qnxsdp-env.sh"; \
		"$$QNX_HOST/usr/bin/qcc" -Vgcc_ntoaarch64le $(CFLAGS) \
			-o motor_recorder recorder.c mqtt_client.c $(LDFLAGS)'

clean:
	rm -f motor_recorder
