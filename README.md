# motor_recorder

Reads the motor controller's shared-memory ring on QNX, writes the rows to CSV,
and publishes over MQTT. It is the third consumer of that ring, alongside
`motor_ai_client` (which forwards batches to the AI service over SOME/IP) and
`shm_chunker`.

Split out of `Qnx_Hypervisor_rbye`, where it lived at `src/motor_recorder`.

## Build

Source the QNX SDP environment, then point the build at the two things that are
not in this repository:

```sh
make MOTOR_HEADERS=../motor_data_producer/QNX-SPI \
     MQTT_INCDIR=/opt/mosquitto/include \
     MQTT_LIBDIR=/opt/mosquitto/lib
```

`motor_wire.h` and `motor_shm.h` come from the producer rather than being copied
in here on purpose: they describe the shared memory layout, and building against
a different copy than the producer was built with is a silent wrong answer
rather than a link error.

Under Yocto this is the `motor-recorder` recipe in `meta-qnx-guest`, which
supplies all three paths from the sysroot and takes `libmosquitto` from the
`mosquitto` recipe in the same layer.

## Run

```sh
motor_recorder [-d <save_dir>]
```

`save_dir` defaults to `/tmp`, which on the QNX guest is RAM — recordings meant
to outlive a reboot want a path on the mounted data disk instead.

## Known rough edges

**The broker address is compiled in.** `MQTT_BROKER` in `mqtt_client.h`, along
with the topic names. Pointing this at a different broker means rebuilding.
`motor_data_producer` reads its equivalent from a `config.json`; this should
probably do the same.

**`mqtt_minimal.c` is not built and not used.** It is a second implementation of
what `mqtt_client.c` does. The Makefile builds `mqtt_client.c` only — linking
both gives duplicate symbols. It is kept because it is the dependency-free
variant, useful if libmosquitto ever becomes inconvenient to carry.
