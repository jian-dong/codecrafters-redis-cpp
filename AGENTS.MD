# Agent Instructions

## Scope

This project implements a simplified Redis server for the CodeCrafters Redis challenge.

Primary goals:
- pass the current CodeCrafters stage
- preserve compatibility with the tester
- keep the implementation easy to extend for later stages

Focus on:
- TCP server behavior
- RESP parsing and encoding
- Redis command handling
- in-memory data storage
- stage-by-stage feature delivery

When behavior is unclear, prefer compatibility with the CodeCrafters tester over full Redis completeness.

---

## Development Rules

- Keep the implementation simple and incremental.
- Implement only what is required for the current stage.
- Do not add future-stage features early unless they clearly simplify the current stage.
- Do not refactor unrelated code while solving a stage.
- Prefer small, testable changes.
- Preserve public entrypoints expected by the starter project and test runner.

---

## Build and Test

- The server must behave like a long-running process and must not exit immediately after startup.
- Use the CodeCrafters CLI for validation.
- Run `codecrafters test` before finishing a change.
- For non-trivial changes, also run `codecrafters test --previous`.
- Use debug logging only when needed for diagnosis.

---

## CLI

- Use `CLI11` for command-line argument parsing.
- Keep command-line options minimal and explicit.
- Do not introduce ad-hoc argument parsing.
- Parse startup configuration in one place only.
- Keep runtime configuration separate from protocol handling and storage logic.

---

## Error Handling

Use explicit error handling throughout the project.

- Use `std::optional<T>` for absence-only results.
- Use `tl::expected<T, E>` for fallible operations with error information.
- Prefer domain aliases in public APIs, such as:
  - `ParseResult<T>`
  - `CommandResult<T>`
  - `IoResult<T>`
- Do not use exceptions for normal control flow.
- Do not silently swallow parse, I/O, or command errors.
- Do not use bare `bool` or sentinel values when richer error semantics are possible.
- Error types should be domain-specific and actionable.

Example:

```cpp
using ParseError = /* ... */;
template <class T>
using ParseResult = tl::expected<T, ParseError>;

using CommandError = /* ... */;
template <class T>
using CommandResult = tl::expected<T, CommandError>;
````

---

## Architecture

Separate the code into clear layers:

* networking
* RESP parsing and serialization
* command dispatch
* storage and state
* configuration and startup

Rules:

* Keep command handlers small and composable.
* Keep RESP parsing isolated from business logic.
* Keep storage independent from socket code.
* Make it easy to add later commands and types without rewriting earlier stages.
* Prefer straightforward state machines over premature abstraction.

---

## Protocol Rules

* Follow the Redis RESP protocol exactly.
* Be strict about response formatting.
* Preserve command semantics expected by the current stage.
* Do not guess protocol behavior when the tester expects exact output.
* Handle malformed input explicitly where needed.
* Avoid mixing transport concerns with protocol parsing.

---

## Data Model

* Use in-memory storage by default.
* Add persistence only when the stage requires it.
* Keep expiration logic isolated from basic key-value storage.
* Add support for new Redis data types only when needed by the challenge.
* Prefer simple internal representations first; optimize later only if necessary.

---

## Unit Testing

In addition to `codecrafters test`, maintain local unit tests for core logic.

Unit tests should cover:

* RESP parsing
* RESP serialization
* command parsing
* command execution on in-memory state
* expiration logic
* malformed input and error cases
* configuration parsing with `CLI11`

Testing rules:

* Prefer unit tests for pure logic and deterministic components.
* Prefer integration tests for socket and server behavior.
* Avoid tests that depend on timing unless unavoidable.
* Do not rely only on end-to-end CodeCrafters tests.
* Every parser or serializer change should include success and failure-path coverage.
* Every new command should include happy-path and error-path coverage.

Preferred testing boundaries:

* protocol logic should be testable without opening a TCP socket
* command logic should be testable without starting the full server

---

## Change Boundaries

* Modify only the code relevant to the current stage.
* Do not rename public entrypoints unless necessary.
* Do not introduce unnecessary dependencies.
* Do not rewrite large files for style cleanup alone.
* Do not add framework-heavy abstractions for a small stage.
* Keep diffs compact and reviewable.

---

## Non-negotiable Rules

* Use `CLI11` for argument parsing.
* Use `std::optional` and `tl::expected` for explicit error handling.
* Add unit tests for protocol and command logic.
* Run `codecrafters test` before finishing.
* Run `codecrafters test --previous` after non-trivial changes.
* Prefer compatibility with the CodeCrafters tester over full Redis parity.

---

## Priorities

1. Pass the current CodeCrafters stage
2. Preserve correctness and protocol compatibility
3. Keep the code extensible for the next stage
4. Maintain strong unit test coverage for core logic
5. Optimize only when needed
