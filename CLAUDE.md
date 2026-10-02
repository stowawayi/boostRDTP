# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build, test, run

Boost (system, filesystem, program_options) and GTest are required and are **not installed system-wide on this machine** — they were provisioned via vcpkg at `C:\Users\stowa\tools\vcpkg`. Configure with that toolchain:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=C:/Users/stowa/tools/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-windows
cmake --build build --config Debug
```

Run all tests:
```bash
ctest --test-dir build
# or directly:
build/tests/Debug/rdtp_tests.exe
```

Run a single test:
```bash
build/tests/Debug/rdtp_tests.exe --gtest_filter=RDTPWireTest.PacketHeaderByteOrder
```

Run the demo binaries against each other (sender first, then receiver — see the role-naming note below):
```bash
build/Debug/rdtp_server.exe <folder-to-send> [bind-address]   # bind-address defaults to "::" (dual-stack)
build/Debug/rdtp_client.exe --host <ip> --port 9000 --output <folder> [--ack-window-size N] [--ack-window-timeout N]
```
(`rdtp_server`'s control channel is always `data-port + 1`, i.e. 9001 by default. The README's `--folder` flag for `rdtp_server` is stale — it takes a positional `<folder>` argument, not a flag.)

## Architecture

**Role naming is inverted relative to the protocol.** `RDTPClient` implements the *receiver* role (connects out to the sender, drives the init handshake, sends acks/nacks) — it's named "Client" only because it opens the outbound connection. `RDTPServer` implements the *sender* role (listens, replies to the handshake, pushes file data, retransmits on NACK) — it's named "Server" only because it listens. Don't assume Client == passive/receiving in the usual sense; check which one calls `send_datagram`/`connect`.

**Two independent channels, not one socket:**
- A UDP data channel carries `PacketHeader` (36 bytes, 9× `uint32_t`, defined in `rdtp.hpp`) followed by the file-segment payload.
- A separate TCP control channel (port = data port + 1 by default) carries all session/reliability messages, each framed by a 16-byte `ControlMsgHeader` (`sync`/`size`/`msg_type`/`cntr`) wrapping one of `InitRequestPayload`, `InitReplyPayload`, or `AckPayload`.

Wire (de)serialization is centralized in `src/rdtp_wire.cpp` (`to_wire()`/`from_wire()` on each struct) — never hand-roll byte layout elsewhere. **Byte order has one deliberate exception**: every field in `PacketHeader` is big-endian *except* `data_type`, which is passed through unswapped — this is intentional, not a bug; do not add a swap for this field. `header_size` is a byte count (36), not a word count.

`RDTPServer`/`RDTPClient` default to dual-stack IPv6 (`bind_address = "::"`); `IPV6_V6ONLY` is explicitly disabled so a `"::"` bind still serves IPv4 peers (mapped to `::ffff:a.b.c.d` before use as a UDP destination — see `RDTPServer::accept()`). `RDTPClient` picks its local UDP socket's address family to match whatever `server_ip` resolves to.

Retransmission state (`tracked_unacked_` in `RDTPServer`) doubles as the source for `oldest_datagram_available`: it's patched into each outgoing packet's header as the current low-water mark at send time, not set by the caller.
