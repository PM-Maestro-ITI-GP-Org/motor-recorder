# motor_recorder -- reads the motor controller's shared-memory ring, writes the
# rows to CSV, and publishes over MQTT.
#
# Source the QNX SDP environment first (qnxsdp-env.sh), then:
#
#     make
#
# Three paths have to be pointed somewhere, because none of them is part of this
# repository:
#
#   MOTOR_HEADERS  motor_wire.h and motor_shm.h, which describe the shared
#                  memory layout. They belong to the producer and are taken from
#                  it rather than copied in here: building against a different
#                  copy than the producer was built with is a silent wrong
#                  answer, not a link error.
#   MQTT_INCDIR    where mosquitto.h is
#   MQTT_LIBDIR    where libmosquitto is
#
# e.g.
#
#     make MOTOR_HEADERS=../motor_data_producer/QNX-SPI \
#          MQTT_INCDIR=/opt/mosquitto/include MQTT_LIBDIR=/opt/mosquitto/lib
#
# The defaults assume a checkout of the producer beside this one and mosquitto
# installed under /usr/local.
#
# This used to source qnxsdp-env.sh itself, from ../../qnx800, and call qcc
# through the $QNX_HOST that produced. That worked only inside the monorepo this
# was split out of, and it took the choice of compiler away from anything
# driving the build. The environment is the caller's job now.

# qcc with the target variant, which is what the SDP environment provides. A
# build system with its own cross compiler overrides this, from the command line
# or the environment.
#
# NOT `CC ?= ...`. make predefines CC as "cc", so its origin is 'default' rather
# than 'undefined' and ?= leaves it alone -- the build then silently uses the
# host gcc and fails on the first QNX header, which reads like a missing SDP:
#
#     recorder.c:15:10: fatal error: sys/neutrino.h: No such file or directory
#
# Testing the origin sets it only when nothing else has, which is what ?= was
# meant to do here.
ifeq ($(origin CC),default)
CC := qcc -Vgcc_ntoaarch64le
endif

MOTOR_HEADERS ?= ../motor_data_producer/QNX-SPI
MQTT_INCDIR   ?= /usr/local/include
MQTT_LIBDIR   ?= /usr/local/lib

# CFLAGS is overridable so a build system can supply its own optimisation and
# warning settings. The two flags this code actually requires are kept out of it
# for that reason: overriding CFLAGS must not be able to drop them.
CFLAGS     ?= -O2 -Wall
APP_CFLAGS := -std=c11 -D_POSIX_C_SOURCE=200809L

CPPFLAGS += -I$(MOTOR_HEADERS) -I$(MQTT_INCDIR)
LDFLAGS  += -L$(MQTT_LIBDIR)

# -lcjson used to be here. Nothing in this program calls cJSON: the one JSON
# document it produces is built with snprintf into a char[4096] in recorder.c.
LDLIBS := -lmosquitto -lsocket -lm

TARGET := motor_recorder

# mqtt_minimal.c is deliberately not built. It is a second, unused
# implementation of what mqtt_client.c does; linking both gives duplicate
# symbols rather than more features.
SRCS := recorder.c mqtt_client.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(APP_CFLAGS) $(CPPFLAGS) -o $@ $(SRCS) $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET)
