# Domain Glossary

## Command

An ordered Redis request consisting of a command name and its arguments.

## Write command

A Command that can modify database state when it succeeds. A successful Write
command participates in persistence and replication.

## Command effects

The deterministic persistence and replication records produced by a successful
Command. A Command can succeed without producing effects, and an effect can
differ from the submitted Command when replay must not repeat blocking,
time-relative, or generated behavior.

`PUBLISH` is a special effect-producing Command: it is propagated to replicas
so their local subscribers receive the message, but it is never persisted in
the append-only log because it does not modify durable database state.

## Transaction

Per-connection state that watches key versions and queues Commands between
`MULTI` and `EXEC`. Execution is aborted when a watched key has changed.

## RESP value

A logical Redis protocol value, independent of its encoded bytes. RESP values
include strings, errors, integers, nulls, arrays, and sequences of values.

## Key absence

The requested key has no live value. Key absence is a normal result, distinct
from a command failure.

## Wrong type

An existing key holds a Redis value type incompatible with the requested
operation. Wrong type is a command failure, distinct from Key absence.

## Redis integer

A string value interpreted by integer commands as a signed 64-bit number. An
invalid representation or an operation outside that range is a command failure.

## Append-only log

An ordered durable log of successful Write commands. Replaying the log restores
database state at startup without generating new log entries.

## RDB snapshot

A point-in-time database image loaded during startup. A snapshot is applied only
after the complete image is known to be structurally valid.

## Replica connection

The long-lived connection from a replica to its master. It owns the handshake,
the replication byte offset, and ordered application of propagated Commands.

## Replica session

The master's live relationship with one downstream replica. It owns ordered
delivery of propagated Commands and tracks the replica's acknowledged offset
until that downstream connection closes.

## Client output

The ordered stream of replies and asynchronous messages sent to one client.
All producers for a connection share this stream, and it stops accepting new
messages when the client session ends.

## Subscription session

Per-connection Pub/Sub state that owns channel and pattern registrations and
enforces subscribed-mode command policy. Publishing itself is normal Command
execution so direct and transactional `PUBLISH` share the same effects path.
