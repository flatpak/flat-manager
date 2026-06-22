# Vendored, patched `actix-multipart` 0.7.2

This is an unmodified copy of `actix-multipart` 0.7.2 from crates.io **except for a
single one-line fix** in `src/field.rs` (`InnerField::read_stream`), wired into the
build via `[patch.crates-io]` in the workspace root `Cargo.toml`.

## The bug

`read_stream` decides whether the start of the payload buffer is a multipart
boundary delimiter with:

```rust
if len > 4 && payload.buf[0] == b'\r' {
```

When a TCP/HTTP chunk boundary lands immediately after the `--` of an inter-part
delimiter, `read_stream` runs with the buffer holding **exactly** the 4 bytes
`\r\n--`. `len > 4` is false, so the "this might be a boundary, wait for more data"
branch is skipped, the parser consumes `\r\n--` as field body, and can no longer
match the real boundary `\r\n--<boundary>`. It misaligns and **silently drops the
following field** — the handler still returns `200`, just with one fewer field.

For flat-manager this surfaced as intermittent `flatpak build-commit-from … error:
Couldn't find file object <hash>` commit failures: the client uploaded every object
but the server dropped one or two from a batch. Probability scales with
(fields × chunk boundaries), so large pushes (e.g. freedesktop-sdk, thousands of
objects) hit it regularly while small app uploads usually don't.

Reproduced deterministically against a real actix-web server: 12 dropped fields per
20000 randomly-chunk-split uploads on stock 0.7.2 **and 0.8.0**; 0 with the patch.

## The fix

```diff
-        if len > 4 && payload.buf[0] == b'\r' {
+        if len >= 4 && payload.buf[0] == b'\r' {
```

`len` 1–3 already fall through to `Poll::Pending`; `len` >= 5 is checked correctly;
`len == 4` was the only unhandled prefix. With `>= 4`, a buffer of exactly `\r\n--`
enters the check, hits `len < b_size`, and returns `Poll::Pending` to wait for the
rest of the delimiter.

## Upstream

This bug is present in actix-multipart through at least 0.8.0. The fix should be sent
upstream (`actix/actix-web`, `actix-multipart/src/field.rs`); once a released version
contains it, drop this vendored copy and the `[patch.crates-io]` entry.
