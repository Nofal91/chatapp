# Go RPC Chat

Concurrent terminal chat: one server, many clients, `net/rpc` over TCP.
Delivery model: **polling** (clients call `Chat.Poll` every 500 ms).

## Run

```bash
go build -o bin/server ./server
go build -o bin/client ./client

./bin/server -addr :1234                      # terminal 1
./bin/client -addr localhost:1234 -name alice # terminal 2
./bin/client -addr localhost:1234             # terminal 3 (prompts for a name)
```

Client commands: `/users` list online users, `/quit` leave, `/help`.
Anything else is broadcast to everyone except you. `Ctrl+C` works on both sides.

## Layout

| Path | Role |
|---|---|
| `chat/chat.go` | RPC contract: arg/reply types, event types, error strings, username rules |
| `server/main.go` | Server: session registry, broadcast queues, reaper, graceful shutdown |
| `client/main.go` | Client: join, poll goroutine, stdin loop, signal handling |
| `server/service_test.go` | Unit + race tests |

## RPC API (`Chat.*`)

| Method | Purpose |
|---|---|
| `Join{Username}` -> `{Token, Users}` | Register; rejects duplicates (case-insensitive) and invalid names |
| `Poll{Token}` -> `{Events}` | Return and clear pending events; also acts as heartbeat |
| `Send{Token, Text}` | Broadcast to all other clients |
| `Users{Token}` -> `{Users}` | List connected users |
| `Leave{Token}` | Disconnect (idempotent) |

Events: `message`, `join`, `leave`, `shutdown`.

## Concurrency design

- `net/rpc` serves each connection in its own goroutine and each call in its own goroutine, so handlers run concurrently.
- All server state (`byToken`, `byName`, per-client queues) is guarded by a single `sync.Mutex`. Broadcast = append to every other client's queue under the lock; no I/O happens while locked.
- Client: one goroutine polls and prints, one goroutine reads stdin, main loop selects on both plus the signal context. A mutex serialises terminal output.
- Run `go test -race ./...` to check.

## Failure handling

| Case | Behaviour |
|---|---|
| Duplicate / invalid username | Server rejects; client re-prompts |
| Client crash or network drop | No poll for 10 s -> reaper evicts, others see "left (connection lost)" |
| Slow client | Queue capped at 1000 events; oldest dropped |
| Client `Ctrl+C` / `/quit` / stdin EOF | Client calls `Leave`, others see "left" |
| Server `Ctrl+C` / SIGTERM | Broadcasts `shutdown`, waits up to 2 s for clients to drain, closes listener and connections; clients print notice and exit |
| Server dies abruptly | Client poll fails 3x (~1.5 s) -> "lost connection", exits |
| Every RPC from client | 3 s timeout so a hung server cannot freeze the client |
| Message hygiene | Control characters stripped server-side (no terminal escape injection); max 1000 chars |
| Session token | Random 128-bit token per join; other users cannot poll or speak as you |
