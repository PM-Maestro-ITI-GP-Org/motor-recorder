/*
 * Self-check for the three helpers that decide what a filename arriving over
 * MQTT is allowed to do.
 *
 * These are the security-relevant ones: before them, `delete ../../x` removed
 * files outside the save directory and `download /etc/shadow` streamed any
 * readable file on the guest out over the broker. A regression here is not a
 * cosmetic bug, so it gets a test rather than a careful reading.
 *
 * recorder.c is #included so the real static functions are exercised. Its
 * main() is renamed out of the way rather than duplicated.
 *
 *   gcc -DMOTOR_NAME_TEST -I<motor headers> -I<mosquitto> -o name_test name_test.c
 */
#define main recorder_main_unused
#include "recorder.c"
#undef main

#include <assert.h>

int main(void)
{
    char out[1024];
    snprintf(g_save_dir, sizeof(g_save_dir), "%s", "/record");

    /* --- names that must be accepted --- */
    assert(resolve_in_save_dir("motor_20260813_200000.csv", out, sizeof(out)) == 0);
    assert(strcmp(out, "/record/motor_20260813_200000.csv") == 0);
    assert(resolve_in_save_dir("run1.csv", out, sizeof(out)) == 0);

    /* --- traversal and absolute paths, the actual bugs --- */
    assert(resolve_in_save_dir("../../etc/passwd", out, sizeof(out)) != 0);
    assert(resolve_in_save_dir("..", out, sizeof(out)) != 0);
    assert(resolve_in_save_dir("/etc/shadow", out, sizeof(out)) != 0);
    assert(resolve_in_save_dir("sub/dir.csv", out, sizeof(out)) != 0);
    assert(resolve_in_save_dir("back\\slash", out, sizeof(out)) != 0);
    assert(resolve_in_save_dir(".hidden", out, sizeof(out)) != 0);
    assert(resolve_in_save_dir("", out, sizeof(out)) != 0);
    assert(resolve_in_save_dir(NULL, out, sizeof(out)) != 0);

    /* A name that cannot fit must be refused, not truncated into some other
       path that happens to exist. */
    char huge[400];
    memset(huge, 'a', sizeof(huge) - 1);
    huge[sizeof(huge) - 1] = '\0';
    assert(resolve_in_save_dir(huge, out, sizeof(out)) != 0);

    /* --- suffix matching: strstr() used to accept these --- */
    assert(has_csv_suffix("run.csv"));
    assert(!has_csv_suffix("run.csv.bak"));
    assert(!has_csv_suffix("run.csvx"));
    assert(!has_csv_suffix("csv"));
    assert(!has_csv_suffix(".csv"));      /* no name, just the extension */

    /* --- JSON escaping: an unescaped quote broke the whole reply --- */
    char esc[256];
    json_escape("plain.csv", esc, sizeof(esc));
    assert(strcmp(esc, "plain.csv") == 0);

    json_escape("we\"ird.csv", esc, sizeof(esc));
    assert(strcmp(esc, "we\\\"ird.csv") == 0);

    json_escape("back\\slash", esc, sizeof(esc));
    assert(strcmp(esc, "back\\\\slash") == 0);

    json_escape("line\nbreak\ttab", esc, sizeof(esc));
    assert(strcmp(esc, "line\\nbreak\\ttab") == 0);

    json_escape("bell\x07here", esc, sizeof(esc));
    assert(strcmp(esc, "bell\\u0007here") == 0);

    /* Never writes past the end, always terminates. */
    char tiny[12];
    memset(tiny, 'Z', sizeof(tiny));
    json_escape("aaaaaaaaaaaaaaaaaaaaaaaaaaaa", tiny, sizeof(tiny));
    assert(tiny[sizeof(tiny) - 1] == '\0' || strlen(tiny) < sizeof(tiny));
    assert(strlen(tiny) < sizeof(tiny));

    printf("name_test: OK (containment, suffix, escaping)\n");
    return 0;
}
