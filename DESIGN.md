# ably-server — Design

`ably-server` is a single Go binary that speaks Ably's realtime WebSocket
protocol and the core REST API, so existing Ably SDKs connect to it
unchanged. It covers pub/sub messaging — including presence and mutable
messages — and runs as one in-memory or on-disk process for local
development and CI, or as a cluster of stateless nodes over a shared
Postgres for self-hosted single-region deployments.

This document describes how it works, section by section.

## 1. Non-goals

- Multi-region / global distribution.
- Ably-cloud-only product surface: integrations / rules, push notifications,
  Spaces, Chat, LiveObjects/LiveSync, message queues, account/app management
  APIs, the `/keys` admin API.
- Statistics collection. `GET /stats` exists purely as a compatibility
  stub — authenticated like any other REST read, gated by the app-wide
  `stats` op (§3.1), always returning an empty array — so SDK flows that
  call it succeed against this server. `POST /stats` is accepted as a
  matching no-op (same auth, drains the body, empty `201`): SDK test flows
  write stats before reading them, and the SDK's write path treats a
  non-2xx as an error whose body it reads, so a `404` would leave it
  blocked. No statistics are collected or stored. Both routes are
  registered only when `--enable-stats-stub` is set (§9); by default
  they are unregistered and `GET`/`POST /stats` fall through to the
  catch-all `40400` (§2.2) like any other unused surface, so a plain
  deployment never exposes routes it doesn't need. The sandbox
  provisioner (§15) enables it for every child, since SDK compat suites
  rely on it.
- Hard durability or HA guarantees beyond what the chosen database provides.
- Backwards compatibility with arbitrary historical Ably protocol versions —
  we target v2 and later.
- Realtime transports other than WebSocket. Comet/HTTP-streaming and SSE
  are restricted-network fallbacks that do not apply to a local dev server
  or a deployment inside the operator's own network; SDKs use WebSocket.
- Token revocation. Revocable tokens presuppose per-token server-side
  state the single-app model does not keep.
- A channel lifecycle/status REST endpoint (occupancy, metadata) — Ably-cloud-only
  product surface.
- Filtered/derived-channel subscriptions — Ably-cloud-only product surface.

## 2. External surface

### 2.1 Realtime (WebSocket)

`GET /` upgrades to WebSocket. The upgrade request carries:

- **Authentication** — via `Authorization` header or `key` / `access_token`
  query parameter (see §3).
- **Protocol version** — required `v=N` query parameter; the server accepts
  `v=2` and above and rejects older versions with an `ERROR` frame and
  close.
- **Format** — `format` query parameter selects `json` (text frames,
  default) or `msgpack` (binary frames).
- **Echo** — `echo` query parameter (default `true`). When `false`, the
  fan-out does not deliver a connection's own published `MESSAGE`s back to
  it; a message is identified as the connection's own by its stamped
  `connectionId`. Presence deliveries are unaffected — a connection always
  receives its own presence messages.

Each frame is a single `ProtocolMessage`. The server emits `CONNECTED` as
the first frame after a successful upgrade.

The `CONNECTED` frame carries a `connectionDetails` object alongside the
top-level `connectionId`, telling the SDK its resolved identity and the
limits to adopt for the connection:

- `clientId` — the resolved `clientId` (§3.2): a concrete value, `*` for a
  wildcard bearer, or omitted for an anonymous connection.
- `connectionKey` — the opaque key an SDK resumes with. Connection-state
  resume is a non-goal (§1, §11), so it is the process-local
  `connectionId` and is not recoverable.
- `maxMessageSize` — `65536` (64 KiB), the largest payload of a single
  publish; SDKs reject oversize publishes client-side and the server
  rejects them with `40009` (§2.2).
- `maxFrameSize` — `524288` (512 KiB), the advertised largest WebSocket
  frame / POST body. The server's hard cap on both is 2 MiB (§2.2).
- `maxInboundRate` — `1000`, the advisory per-connection publish ceiling
  in messages/second.
- `connectionStateTtl` — `120000` ms, how long an SDK treats the
  connection state as recoverable after an abrupt disconnect.
- `maxIdleInterval` — the server heartbeat cadence in milliseconds (the
  longest the server leaves the server→client direction idle before
  emitting `HEARTBEAT`).

`maxMessageSize` is enforced (§2.2); the others are advisory: the server
publishes them for SDK consumption but does not itself enforce them.

Supported `Action` values:

| Action | In | Out | Notes |
|---|---|---|---|
| `HEARTBEAT` (0) | ✓ | ✓ | server-driven keepalive + client pings |
| `ACK` (1) | | ✓ | publish acknowledgement for `msgSerial` |
| `NACK` (2) | | ✓ | publish rejection for `msgSerial` |
| `CONNECTED` (4) | | ✓ | first frame after upgrade; carries `connectionId` |
| `DISCONNECT` (5) | ✓ | | client signalling intent to disconnect |
| `DISCONNECTED` (6) | | ✓ | server-initiated disconnect with reason |
| `CLOSE` (7) | ✓ | | client requests a clean connection close |
| `CLOSED` (8) | | ✓ | server confirms close |
| `ERROR` (9) | | ✓ | |
| `ATTACH` (10) | ✓ | | client requests channel attach (see §4) |
| `ATTACHED` (11) | | ✓ | attach ack (see §4) |
| `DETACH` (12) | ✓ | | client requests channel detach |
| `DETACHED` (13) | | ✓ | detach ack |
| `PRESENCE` (14) | ✓ | ✓ | presence enter/update/leave + delivery (see §12) |
| `MESSAGE` (15) | ✓ | ✓ | publish + delivery |
| `SYNC` (16) | | ✓ | presence set sync after attach (see §12) |
| `ANNOTATION` (21) | ✓ | ✓ | annotation publish + delivery (see §14) |
| `AUTH` (17) | ✓ | ✓ | inband re-auth: the server prompts near token expiry; the client supplies a fresh token (see §3) |

### 2.2 REST

All REST endpoints live under the root and accept either `application/json` or
`application/x-msgpack` for both request and response bodies (driven by
`Accept` / `Content-Type`).

| Method | Path | Purpose |
|---|---|---|
| POST | `/channels/{channel}/messages` | publish (create) 1..N messages |
| PATCH | `/channels/{channel}/messages/{serial}` | update / delete / append (action in body, see §13) |
| GET | `/channels/{channel}/messages` | history (paginated; latest version per message, see §13.4) |
| GET | `/channels/{channel}/messages/{serial}` | one message, latest version (see §13.4) |
| GET | `/channels/{channel}/messages/{serial}/versions` | all versions of a message (paginated) |
| POST | `/channels/{channel}/messages/{serial}/annotations` | publish an annotation on a message (see §14) |
| GET | `/channels/{channel}/messages/{serial}/annotations` | a message's annotations (paginated, see §14.4) |
| GET | `/channels/{channel}/presence` | current presence members (see §12) |
| GET | `/channels/{channel}/presence/history` | presence history (paginated) |
| POST | `/keys/{keyName}/requestToken` | mint a token (JWT) from a signed `TokenRequest` (see §3) |
| GET | `/stats` | compatibility stub: always an empty array; only registered with `--enable-stats-stub`, else `40400` (see §1, §9) |
| POST | `/stats` | compatibility no-op: accepts and discards, empty `201`; same gating as GET (see §1, §9) |
| GET | `/time` | server time (ms since epoch) |
| GET | `/healthz` | liveness — no auth, dependency-free, 200 once serving |
| GET | `/readyz` | readiness — no auth; 200 in `memory`/`disk` mode; in `cluster` mode 503 unless Postgres answers a ping, the bus is connected (`--bus=nats`: NATS; `--bus=postgres`: LISTEN; §7.2) and every publish lane is completing its commits (§11) |

A successful publish returns `201` with a `{"channel": "<name>",
"messageId": "<id>", "serials": ["<serial>", …]}` body (msgpack when the
`Accept` header requests it). `messageId` is the stamped id of the
publish's first message — `"<batchID>:0"` (§8) — the same id carried on
the delivered `MESSAGE` frame. `serials` carries one entry per published
message, in batch order, each the message's stable identity `serial` (§8)
— the value a client uses to address the message via `PATCH` / `GET
.../messages/{serial}` (§13), and what the SDK's `PublishWithResult`
surfaces.

**Request and message size limits.** Every REST request body is read
through `http.MaxBytesReader` with one cap, `protocol.MaxRequestBodyBytes`
(2 MiB): publish, mutation, annotation, `requestToken` and the discarded
`POST /stats` body. A declared `Content-Length` over the cap is refused
before any byte is read; a chunked or under-declared body is cut off by
the reader. Both return `413` with Ably code `40009` ("request body too
large"). The same 2 MiB bounds an inbound WebSocket frame (the connection
closes with 1009). The cap is a transport guard well above the message
limit; it is not the limit SDKs see. The message limit is
`maxMessageSize` (64 KiB, §2.1): the sum over the messages of one publish
of the name, `clientId`, decoded data and extras lengths in bytes (Ably's
TM6 size). A REST publish or mutation over it returns `400` with `40009`
and a realtime `MESSAGE` over it is NACKed `40009`, in both cases before
anything is queued or stored, so the lane queues (§6.3) only ever hold
messages within the limit.

**Keep-alive and the publish fast path.** REST publishers are expected
to reuse connections: the HTTP server keeps an idle keep-alive connection
open for `--http-idle-timeout` (default 120 s) and then closes it, so idle
connections are not held forever. A client driving high publish rates
should raise its per-host idle connection pool to its concurrency (Go's
`http.Transport` keeps only 2 idle connections per host by default,
`MaxIdleConnsPerHost`); otherwise every request beyond that opens a new
TCP connection. The common publish, one JSON message with Basic key
auth, takes a fast path that changes no behaviour:

- Basic auth on a request with no query string compares the
  `Authorization` header in constant time against each key's precomputed
  header and returns a principal built once per key, with no decoding or
  allocation. Any other credential form takes the general path.
- A body that is a single JSON object whose members are plain strings
  among `name`, `data`, `id`, `clientId`, `encoding` and `connectionKey`
  (no escape sequences) is decoded without reflection, then normalised
  exactly as `Message.UnmarshalJSON` would (for example base64 data).
  Every other body, including any malformed one, is decoded by
  `encoding/json` as before; a fuzz test checks the two agree.
- The body is read into one buffer of its declared `Content-Length`, and
  a JSON response whose strings need no escaping is written directly,
  byte-identical to `encoding/json`'s output.

Measured on a laptop (±50%), with a storage stub so the numbers are the
REST path's own, the in-process handler went from about 2.4 µs, 42
allocations to about 1.2 µs, 27 allocations per publish (half of what is
left is building the request); over loopback HTTP the time is dominated by
the kernel.

Pagination follows Ably's `Link` header convention (`first`, `next`), each
rel emitted as its own `Link` header line with a URL relative to the
requested resource (its final path segment plus query), matching how Ably
SDKs resolve continuation links.

An unknown resource — an unrecognised path, or a known path under a method
Ably treats as a missing resource rather than a method error (e.g. `GET` on
the `POST`-only `requestToken`) — returns `404` with the Ably error body
`{"error":{"code":40400,"statusCode":404,"message":...}}` in the `Accept`
format, alongside the `X-Ably-Errorcode` / `X-Ably-Errormessage` headers
SDKs read for the code and message.

## 3. Authentication & authorisation

One or more API keys are configured (via repeated `--keys`, a
comma-separated `ABLY_SERVER_KEYS`, or the config file — §9), each in
the canonical Ably format `appId.keyId:keySecret` (so SDKs that parse the
key work unchanged). Each key carries a **capability** (§3.1): a key
configured via a flag or an env var grants the full `{"*":["*"]}`
capability, while a structured `[[keys]]` config entry (§9) may narrow it
to a scoped set. A Basic-auth holder of a
key resolves to that key's capability, and a token minted or signed by a
key is bounded by it (§3.3). At least one key is required; the server
refuses to start with none. All configured keys must share the same
`appId`: the
server owns one channel namespace (mirroring how one Ably app owns one
namespace), so keys spanning multiple appIds are a misconfiguration and
startup fails. A request authenticates against **any** configured key,
and JWT verification selects the signing key by the token's `kid` header
(falling back to trying every key's secret when `kid` is absent or names
no configured key). The server **verifies** tokens presented on
connect/request and also **issues** them on demand via
`POST /keys/{keyName}/requestToken` (§3.3).

Two accepted credential forms:

1. **Basic auth** — `Authorization: Basic base64(appId.keyId:keySecret)` or
   `?key=...`. Carries the API key's full capability.
2. **JWT** — bearer token signed with `keySecret` (HS256). Passed on REST as
   `Authorization: Bearer base64(jwt)` (Ably base64-encodes the header value,
   RSA3a — a raw token is also accepted) or, on the WS upgrade, as the
   `?access_token=...` query param (`?accessToken=...` is also accepted).

JWT claims:

| Claim | Required | Purpose |
|---|---|---|
| `iat` | ✓ | issued-at; a token issued in the future beyond a small clock-skew leeway is rejected |
| `exp` | ✓ | expiry; checked with no grace, so an expired token is rejected the instant it lapses |
| `x-ably-capability` | | JSON object granting per-channel ops (see §3.1). Present → the token's capability is the claim **intersected with** the signing key's capability. Absent → the token inherits the signing key's capability outright (for a full-capability key that is `{"*":["*"]}`, so the claim is only needed to *narrow* access; for a scoped key the token is bounded by the key regardless) |
| `x-ably-clientId` | | string; controls the connection's `clientId` (see §3.2) |

**Inband re-authentication.** A token-authenticated WebSocket tracks its
token's `exp`. About 30 seconds before expiry the server sends an `AUTH`
frame prompting the client to renew (immediately if the token is adopted
already inside that window); the client replies with an `AUTH` frame
carrying a fresh token in `auth.accessToken`. The server verifies it and
requires the new credential to be **compatible** with the connection — the
resolved `clientId` (§3.2) must be unchanged — then swaps in the new
capability set and expiry and replies with a `CONNECTED` frame carrying
updated `connectionDetails`, all without dropping the connection.

A token whose remaining lifetime on adoption is below a small margin (~5.5
seconds) is **not** prompted: it is simply left to expire. The two failure
modes are deliberately distinct so the SDK reacts correctly:

- **Token lapsed** (no valid token by `exp`) — the server sends a
  `DISCONNECTED` frame with the token-expired code `40142` (status `401`).
  This is renewable: the SDK obtains a fresh token and reconnects (RTN15h2,
  RTN22a).
- **Client-supplied token rejected** (an inband `AUTH` whose token fails to
  verify, carries no token, or resolves to an incompatible `clientId`) — the
  server sends an `ERROR` frame with a non-renewable credential error
  (`40101` invalid credentials, `40102` incompatible; status `401`). The
  code sits outside the SDK's renewable token-error range, so the SDK moves
  the connection to `FAILED` rather than looping on reconnect (RTC8a2).

### 3.1 Capabilities

`x-ably-capability` is a JSON object of the form
`{ "<resource>": ["<op>", ...] }` where:

- `<resource>` is a channel name pattern. The server uses Ably's standard
  wildcard semantics ([docs](https://ably.com/docs/auth/capabilities#wildcards)):
  wildcards replace whole `:`-delimited segments, a `*` at the **end** of
  the pattern can match any number of trailing segments, and elsewhere
  matches exactly one. So `*` matches every channel, `foo:*` matches
  `foo:bar` and `foo:bar:baz`, and `foo:*:baz` matches `foo:bar:baz` but
  not `foo:bar:bam:baz`. `foo*` (no `:` before the `*`) is a literal
  channel name. Character classes (`[a-z]`) and `**` are not supported.
  A resource may carry a leading `[qualifier]` prefix scoping the resource
  TYPE (`[qualifier]name`); the qualifier is parsed and matched per Ably
  semantics. `[*]` matches any type, so the standard sandbox all-access key
  `{"[*]*":["*"]}` grants everything a plain channel needs (equivalent to
  `{"*":["*"]}` over the channel surface here). A concrete qualifier such
  as `[queue]*` / `[meta]*` parses but matches no channel, since neither
  queues nor metachannels exist as resources. When intersecting
  capabilities (§3.3) a `[*]` qualifier on either side yields the other's,
  and two differing concrete qualifiers do not intersect.
- `<op>` is one of `publish`, `subscribe`, `presence`, `history`,
  `stats`, `annotation-publish`, `annotation-subscribe`,
  `message-update-own`, `message-update-any`,
  `message-delete-own`, `message-delete-any`. `*` matches any op.
  The `annotation-*` ops gate annotations (§14): publishing one, and
  receiving the raw `ANNOTATION` stream (summaries need only
  `subscribe`).
  `stats` gates the `/stats` stub (§1) and is app-wide rather than
  per-channel, so it must be granted on the `*` resource.
  `subscribe` covers both
  receiving messages and receiving presence (events + sync + the current
  set); `presence` covers registering presence (enter/update/leave);
  `history` covers message history, message version history, and
  presence history. The `message-{update,delete}-{own,any}` ops gate
  mutable messages (§13.5): `-own` permits the operation only when the
  caller's resolved `clientId` matches the target message's creator,
  `-any` waives that check; `append` is gated by `message-update-*`.

Every authenticated request resolves to a **capability set**. For each
operation the server computes the union of granted ops across all matching
resources and checks that the requested op is in the union. If not, the
operation is rejected:

| Surface | Op required |
|---|---|
| WS `ATTACH` flag `SUBSCRIBE` | `subscribe` |
| WS `ATTACH` flag `PUBLISH` | `publish` |
| WS `ATTACH` flag `PRESENCE` | `presence` |
| WS `ATTACH` flag `PRESENCE_SUBSCRIBE` | `subscribe` |
| WS inbound `MESSAGE` (`create`) | `publish` (and the attachment must hold the `PUBLISH` mode flag, granted at attach time) |
| WS inbound `MESSAGE` (`update`/`append`) | `message-update-own`/`-any` (see §13.5) |
| WS inbound `MESSAGE` (`delete`) | `message-delete-own`/`-any` |
| WS inbound `PRESENCE` | `presence` (and the attachment must hold the `PRESENCE` mode flag) |
| WS `ATTACH` flag `ANNOTATION_PUBLISH` | `annotation-publish` |
| WS `ATTACH` flag `ANNOTATION_SUBSCRIBE` | `annotation-subscribe` |
| WS inbound `ANNOTATION` | `annotation-publish` (and the attachment must hold the `ANNOTATION_PUBLISH` mode flag) |
| REST `POST .../messages` | `publish` |
| REST `PATCH .../messages/{serial}` | `message-{update,delete}-{own,any}` (see §13.5) |
| REST `GET .../messages`, `GET .../messages/{serial}[/versions]` | `history` |
| REST `GET .../presence` | `subscribe` |
| REST `GET .../presence/history` | `history` |
| REST `POST .../messages/{serial}/annotations` | `annotation-publish` |
| REST `GET .../messages/{serial}/annotations` | `history` |
| REST `GET /stats` | `stats` — app-wide, so it must be granted on the `*` resource |

`ATTACH` mode resolution: the effective mode set delivered in `ATTACHED.flags`
is `requested ∩ capability-permitted`. Empty intersection → `ERROR` with
`code: 40160` (insufficient capabilities) and the channel is not attached.

### 3.2 Client ID

A connection (or REST request) resolves to one of three identities:

- **concrete** `clientId` — the server stamps it onto every outbound
  `Message.clientId` that omits one, and rejects (`NACK`) any inbound
  message asserting a *different* one;
- **wildcard** (`*`) — the bearer may assume any identity, chosen per
  operation: each message carries its own `clientId`, stamped through
  unchanged, and a message with none stays unidentified;
- **none** (anonymous) — the bearer may assert no identity; a message
  carrying any `clientId` is rejected.

Resolution depends on the credential and the `clientId` query parameter on
the upgrade (WS) or request (REST):

| Credential | `x-ably-clientId` claim | `clientId` query param | Resolved `clientId` |
|---|---|---|---|
| Basic | n/a | absent | `*` (wildcard — a key may assume any identity) |
| Basic | n/a | `<value>` | `<value>` |
| JWT | absent | absent | none (anonymous) |
| JWT | absent | `<value>` | rejected (no permission to assert clientId) |
| JWT | `<concrete>` | absent | `<concrete>` (claim value) |
| JWT | `<concrete>` | `<concrete>` matching claim | `<concrete>` |
| JWT | `<concrete>` | `<value>` ≠ claim | rejected |
| JWT | `*` | absent | `*` (wildcard) |
| JWT | `*` | `<value>` | `<value>` |

`*` is a wildcard marker meaning "the bearer may assume any `clientId`". It
is **never** itself used as a `clientId` on a message or as a member
identity; to act under a fixed identity the bearer selects a concrete
value via the `clientId` query parameter.

A connection or REST request that fails the table above is rejected at
auth time — the WS upgrade or REST request returns `401`.

On the REST publish path this resolved identity is applied per message
exactly as on the realtime path: a concrete `clientId` is stamped onto a
published message that omits one (RSL1m1 — SDKs deliberately elide the
implicit `clientId`), a wildcard/anonymous identity stamps nothing (and
never the literal `*`), and a message asserting a disallowed `clientId` is
rejected with `40012`. A REST publish may additionally carry a
`connectionKey` to publish on behalf of a live realtime connection: the key
is resolved to that connection and its `connectionId` alone is stamped (never
its `clientId` — a message's `clientId` always comes from the REST request's
own resolved identity above), then the key is stripped so it is never
persisted or delivered. An unresolvable key — malformed, or naming a
connection on another cluster node (the registry is per-node, §8) — is
rejected with `40006`.

Presence imposes a further requirement at *use* time rather than auth
time: a member must be identified, so a connection that resolved to no
`clientId` (anonymous) cannot enter presence, and a wildcard bearer must
select a concrete `clientId` to enter (see §12.3).

### 3.3 Token requests

`POST /keys/{keyName}/requestToken` mints a token for a key holder. The
body is an Ably `TokenRequest` (`keyName`, `ttl`, `capability`, `clientId`,
`timestamp`, `nonce`, `mac`); `ttl` and `timestamp` are accepted as either
a JSON number or a numeric string (the `authUrl` exchange reassembles a
signed request from query params, so they arrive as strings). The request
is authenticated one of two ways:

- **Signed** — the `mac` is `base64(HMAC-SHA256(text, keySecret))` over the
  canonical text `keyName"\n" ttl"\n" capability"\n" clientId"\n"
  timestamp"\n" nonce"\n"` (`ttl` empty when unset). The server recomputes
  it and compares in constant time.
- **Basic, same key** — a request without a `mac` is accepted only when it
  also carries Basic auth for the same key (the holder is explicitly
  authenticated, so the mac is unnecessary).

An authenticated request is then validated (client errors, not server
errors):

- **`capability`** — must be well-formed: parseable JSON, every op list
  non-empty, `*` never combined with another op, and every op a recognised
  Ably operation name. A violation is a `400` (`40000`). (A configured
  key's own capability is trusted and not subject to this check, so it may
  carry ops this server does not itself enforce, e.g. push.)
- **`ttl`** — a negative ttl or one above the 24-hour maximum is a `400`; a
  non-numeric ttl is a `400`. Zero/absent means the 60-minute default.
- **`timestamp`** — must be within two minutes of server time, else `40104`
  (`401`) — guarding against stale or replayed requests. A signed request
  must carry one at all: since the mac is computed over the timestamp, a
  signed request omitting it would be replayable indefinitely, so a
  signed request with `timestamp` absent or zero is rejected as
  not-current (`40104`) rather than skipping the check. A Basic-auth
  request may omit it — the key holder is already proven by the Basic
  credential, so there is nothing here for replay protection to guard.
- **`nonce`** — a nonce reused within the timestamp window is a replay,
  rejected with `40105` (`401`). For the same reason as `timestamp`, a
  signed request must carry a `nonce`; a signed request with it absent is
  rejected (`40105`), while a Basic-auth request may omit it. Seen nonces
  are tracked in an in-memory set per server process and evicted once
  past the window (a later reuse fails the timestamp check regardless).
  This makes replay detection **per-node best-effort**: it does not
  extend across cluster nodes (a captured signed request replayed against
  a different node within the timestamp window mints a fresh token) or
  across a node restart. This is acceptable for the intended deployment
  model — a single client or a locked-down environment — where
  cross-node request replay is not a realistic threat; developers wanting
  stronger guarantees are encouraged to sign their own JWTs rather than
  use `requestToken`, sidestepping the replay window entirely.

The minted token is an **HS256 JWT** signed with the key secret, `kid` set
to the key name, carrying `iat`/`exp` (from `ttl`, default 60 min), the
request `nonce` as `jti` (so distinct requests yield distinct tokens), and
`x-ably-capability` / `x-ably-clientId` when supplied. `exp` carries
sub-second precision, so a short (sub-second) ttl yields a token that
actually expires when it should rather than lasting up to a whole second.
It is returned in a `TokenDetails` response body (`token` holds the JWT),
which the SDK then presents as an `access_token` verified by the path
above.

The requested `capability` is *narrowed* against the signing key's
capability before it is stamped on the token (§3.1): the token grants
only the intersection of what was requested and what the key permits. A
full-capability key (`{"*":["*"]}`) passes a requested capability through
unchanged; a scoped key clamps it to the key's own grants; either way a
request whose capability the key cannot grant at all (empty intersection)
is rejected with `40160` (`401`). A request with no capability leaves the
claim unset, so the minted token inherits the key's capability when it is
later verified. The `TokenDetails` response always carries the **granted**
capability as a JSON string — the narrowed value stamped on the token, or
the key's own capability verbatim when none was requested — so the SDK can
always parse it.

## 4. Attachments

An **attachment** is the relationship between one connection and one channel.
It is created by an inbound `ATTACH` and torn down by `DETACH`, by the
connection closing, or by an unrecoverable error.

A channel name must be **valid** before any attachment or publish is
accepted: non-empty, not starting with `:`, `,`, an ASCII whitespace
character, or `[` (the last excluded because this server does not
implement qualified/scoped channels — a reference-server name starting
with `[` denotes one), and containing no Unicode line-break character
(`\n`, `\v`, `\f`, `\r`, NEL U+0085, LS U+2028, PS U+2029) anywhere in the
name. An invalid name is rejected with Ably error `40010` — as an `ERROR`
reply to `ATTACH`, as a NACK on a publish that has no prior attach, and as
a `400` from the REST publish endpoint — before any other processing of
the request.

### 4.1 Lifecycle

```
client                        server
  │ ── ATTACH(channel, flags, params, channelSerial?) ──▶
  │                              │ resolve cap ∩ flags  → effective modes
  │                              │ open/find Channel
  │                              │ choose attach point
  │   ◀───── ATTACHED(flags, channelSerial) ────│
  │   ◀───── MESSAGE × N (replay, if any) ──────│
  │   ◀───── MESSAGE … (live) ───────────────────│
  │
  │ ── DETACH ──▶
  │                              │ remove from Channel
  │   ◀──── DETACHED ────────────│
```

A repeat `ATTACH` for a channel the connection is already attached to is an
**in-place mutation** of that attachment, not a no-op and not a
detach+re-attach: the SDK sends `ATTACH` whenever the channel is not locally
`ATTACHED`/`ATTACHING` and blocks until it sees an `ATTACHED` (RTL4), and it
re-`ATTACH`es to change modes (`setOptions`). The server re-resolves the
effective modes and params from the new `ATTACH` (flags/params ∩
capability), applies them to the live attachment, and replies `ATTACHED`
with the `RESUMED` flag set, the current `channelSerial`, and the negotiated
modes/params — without disturbing the message stream or its delivery
goroutine. Because the underlying subscription is never interrupted,
continuity holds by construction: no message is missed and none is
duplicated. Presence is **not** resynced on an in-place re-attach. An
explicit backwards `channelSerial` cursor on a live attachment is ignored
for now (it exists to drive delta-fill recovery, deferred with delta
support); the reply is at the current position, which is safe because this
server never emits deltas.

`ATTACHED` is the **first** frame the server emits in response to `ATTACH`;
any replay or live messages follow it. Its fields:

- `flags` — the **effective** mode set (see §4.2). The SDK reads
  `channel.modes` from these bits (RTL4m).
- `channelSerial` — the **confirmed attach point**: the serial *from which*
  the message stream the client is now subscribed to begins. Subsequent
  `MESSAGE` frames carry their own serials advancing from that point, and
  the SDK uses the most recent serial it has seen as its cursor for any
  future re-`ATTACH`.
- `params` — the channel params the client requested, echoed back so the
  SDK can populate `channel.params` (RTL4k1). The `modes` entry, when the
  client requested modes via the params map, is rewritten to the effective
  mode set (§4.2); other requested params (e.g. `rewind`) pass through
  unchanged. A re-`ATTACH` (including one triggered by `setOptions`) updates
  the attachment in place, so the new `ATTACHED` reflects the updated params
  and modes.

### 4.2 Modes

A client requests modes two ways: the `ATTACH.flags` bitfield, or a
comma-separated `modes` channel param (e.g. `params.modes =
"subscribe,presence"`, how ably-js sends `ChannelOptions.params.modes`).
The `modes` param takes precedence over the flags mode bits, which take
precedence over the default set; an unrecognised param token is ignored.
The mode bits occupy the high end of the flags word, matching Ably's wire
constants:

| Mode | Bit | Grants |
|---|---|---|
| `PRESENCE` | `1 << 16` | enter/update/leave presence (see §12) |
| `PUBLISH` | `1 << 17` | publish `MESSAGE` |
| `SUBSCRIBE` | `1 << 18` | receive `MESSAGE` |
| `PRESENCE_SUBSCRIBE` | `1 << 19` | receive `PRESENCE` + presence sync |
| `ANNOTATION_PUBLISH` | `1 << 21` | publish `ANNOTATION` (see §14) |
| `ANNOTATION_SUBSCRIBE` | `1 << 22` | receive raw `ANNOTATION` frames (see §14.3) |

If `flags` carries no mode bits the server treats it as the default set —
`PRESENCE`, `PUBLISH`, `SUBSCRIBE`, `PRESENCE_SUBSCRIBE`, and
`ANNOTATION_PUBLISH` (matching the reference's `MODE_DEFAULT` and the SDK's
expected default `channel.modes`). Only `ANNOTATION_SUBSCRIBE` is opt-in —
raw annotation delivery must be requested explicitly (§14.3).

The effective mode set is `requested ∩ capability-permitted`, where the
permitted set is derived from the per-op capability mapping in §3:

- `SUBSCRIBE` and `PRESENCE_SUBSCRIBE` permitted iff cap grants
  `subscribe` on the channel.
- `PUBLISH` permitted iff cap grants `publish`.
- `PRESENCE` permitted iff cap grants `presence`.
- `ANNOTATION_PUBLISH` / `ANNOTATION_SUBSCRIBE` permitted iff cap grants
  `annotation-publish` / `annotation-subscribe`.

Empty intersection → the attach is rejected with `ERROR` (`code: 40160`)
and no channel state is created. Otherwise `ATTACHED.flags` carries the
effective set, plus the status flags:

- `HAS_PRESENCE` (`1 << 0`) when the channel has a non-empty presence set,
  so the SDK knows a `SYNC` will follow (§12.4).
- `HAS_BACKLOG` (`1 << 1`) when the attach replayed any backlog ahead of
  live delivery — a `rewind` or a resume gap-fill (§4.3). The SDK surfaces
  it as `ChannelStateChange.hasBacklog` (RTL2i); it is cleared on a fresh
  attach that replayed nothing.
- `RESUMED` (`1 << 2`) — see §4.3.

When the modes were requested via the `modes` param, the
effective set is also echoed back in `ATTACHED.params.modes` (§4.1).

Once attached, modes gate frame flow:

- An attachment without `SUBSCRIBE` does not receive `MESSAGE` frames.
- An attachment without `PRESENCE_SUBSCRIBE` receives neither live
  `PRESENCE` frames nor the post-attach `SYNC`.
- Inbound `MESSAGE` from an attachment without `PUBLISH` is rejected with
  `NACK`.
- Inbound `PRESENCE` from an attachment without `PRESENCE` is rejected
  with `NACK`.

### 4.3 Replay (`channelSerial` and `rewind`)

The v2 protocol holds **no per-connection server state across disconnects**.
There is no recovery TTL, no retained outbox, no resume buffer. Connection-
level `recover` / `resume` parameters are accepted on the upgrade URL for
SDK compatibility but never resume connection state: a reconnect always
gets a **fresh `connectionId`**, so connection-state resume/recovery is an
accepted incompatibility (the SDK's connectionId-continuity checks —
ably-go RTN15c6, RTN16f — do not hold against this server).

The server still declines a resume/recover *per protocol* so the SDK reacts
cleanly rather than silently mis-accounting a phantom resume:

- A **malformed** `resume` / `recover` key (not a well-formed connectionId
  this server could have issued) yields `CONNECTED` with the fresh
  `connectionId` **plus an error** (`80018`, status `400`). Because the
  connectionId differs *and* an error is present, the SDK treats the resume
  as failed: it resets its `msgSerial`, resends pending publishes, and
  re-attaches its channels (RTN15c7, RTN16e).
- A **well-formed** key is left un-errored. The server cannot truly resume
  it (it holds no connection state) but does not know it from a genuine
  resume, so it starts a fresh connection without signalling failure; the
  SDK recovers message flow through per-channel re-attach (below) rather
  than connection resume.

Continuity instead lives at the attachment level, driven by the client:

- The SDK tracks `channelSerial` from `ATTACHED` and every inbound
  `MESSAGE` for each attachment.
- On a fresh `ATTACH` (after reconnect, or detach/attach) it supplies the
  last-seen `channelSerial`.
- The server picks an attach point at-or-before that serial, returns it in
  `ATTACHED.channelSerial`, and then streams the messages from that point
  onwards (the gap between the client's cursor and the live head, followed
  by live traffic).
- If the supplied serial is older than retained history (the relevant
  messages are no longer held in storage — see the retention note in §6)
  the server still attaches: it picks the channel's current head as the attach
  point, clears `ATTACHED.flags.RESUMED`, and populates `ATTACHED.error`
  with an `ErrorInfo` (code 80016, "unable to recover channel (messages
  expired)") explaining that the requested resume could not be satisfied
  so the SDK can surface a discontinuity to the application. No replay is
  delivered in this case. "Older than retained history" is decided by
  time, not by what happens to be left in storage: a cursor whose mint
  time is before now minus the channel's retention (§6.3) cannot be proven
  continuous, because cms between it and the oldest retained one may have
  been dropped. Backends that do not enforce retention (memory, disk)
  never take this path.
- The same signal can come mid-stream, without a new `ATTACH`: when a
  cluster node cannot prove from the log that it delivered every cm on a
  channel (§7.2), each attachment receives an `ATTACHED` at its current
  `channelSerial` with `RESUMED` clear and error 80016, a
  server-initiated channel update (RTL12): the SDK emits `update` with
  `resumed: false`. Nothing is replayed; live delivery carries on.

`rewind` is the only honoured channel param. `rewind=N` (positive integer)
selects an attach point N messages before the live head; `rewind=<duration>`
(e.g. `15s`, `2m`) selects the attach point at the start of that duration.
The `ATTACHED.channelSerial` reflects the resulting attach point and the
historical messages then stream as ordinary `MESSAGE` frames. All other
channel params are silently ignored.

Whenever an attach replays a non-empty backlog — a `rewind` window or a
resume gap-fill — the `ATTACHED` sets the `HAS_BACKLOG` flag (§4.2) so the
SDK can surface `hasBacklog` (RTL2i) before the replayed frames arrive.

`rewind` and `channelSerial` are mutually exclusive on a single `ATTACH`:
if both are supplied, `channelSerial` wins (it is the more precise cursor)
and `rewind` is ignored. `rewind` is likewise suppressed when the `ATTACH`
is a resume — either a supplied `channelSerial` or the `ATTACH_RESUME`
flag (`1 << 5`, RTL4j, set by the SDK on a non-clean attach): a
continuation must not replay history the client has already seen.

### 4.4 Implementation

An attachment is a per-node (connection, channel) pair, structured as
a cursor over the Channel's linked list of entries (§5.1). A single
goroutine per attachment walks the cursor — parking on one of the
current entry's wake channels until the next entry is linked, then
advancing and forwarding the ChannelMessage. `forward` writes one
`MESSAGE` `ProtocolMessage` per ChannelMessage onto the connection's
outbound queue (with `ChannelSerial = cm.ChannelSerial`, `Messages =
cm.Messages`, gated by mode flags), encoded once per wire format for all
the channel's attachments when no attachment needs its own version of
it (§5.1). The connection's single writer
goroutine (§5.2) serialises actual frame writes. There is no
per-attachment buffered fan-out channel: each attachment proceeds at
its own pace, lagging the live tail with no upper bound but its own
memory footprint.

The starting cursor depends on how the attachment was created:

- Fresh attach (no `channelSerial`, no `rewind`) — `a.e = channel.Tail()`,
  so the first iteration parks on the tail and wakes on the next live
  publish.
- Resume by `channelSerial` — the attachment first reads the gap from
  history storage, walking those messages directly (without going through
  the linked list), then transitions to the live list at the resume
  point.
- `rewind=N` / `rewind=<duration>` — same pattern: read the historical
  prefix from storage, then attach to the live tail.

## 5. Internal architecture

```
                    ┌─────────────────────────────────────┐
                    │              ably-server             │
                    │                                      │
   client ── WS ───▶│  realtime/  ── ConnectionLoop ──┐   │
                    │      │                          │   │
   client ── HTTP ─▶│   rest/   ── Handlers ──────────┤   │
                    │      │                          │   │
                    │      ▼                          ▼   │
                    │   ┌─────────────────────────────────┴──┐
                    │   │       core/Channel manager          │
                    │   │  - Channel: linked list of entries  │
                    │   │  - Attachments: cursors on the list │
                    │   └─────────────────────────────────────┘
                    │            │                ▲           │
                    │            ▼                │ NOTIFY    │
                    │      Storage iface  ────────┘ (cluster) │
                    │            │                            │
                    └────────────┼────────────────────────────┘
                                 ▼
                       memory / disk / database
```

Three deployment modes are selected by configuration, differing only in
where state and pub/sub live:

1. `memory` — single process, in-memory state, in-memory pub/sub.
2. `disk` — single process, on-disk persistence, in-memory pub/sub.
3. `cluster` — N processes, shared database for both state and pub/sub.

Server processes are stateless: any node can serve any connection, with no
peer-to-peer membership or gossip. The mechanics of each mode are detailed
in §6 (storage) and §7 (pub/sub).

Protocol types (`ProtocolMessage`, `Action`, `Message`, `PresenceMessage`)
are defined in `internal/protocol/`, with their field tags and
action/flag constants pinned to ably-go's wire constants so SDKs
interoperate unchanged. (The original plan was to import
`github.com/ably/ably-go/ably/proto` directly and fall back to local
definitions only on friction; in practice the local definitions carry the
whole surface, so the package owns them outright.)

Major packages:

```
cmd/ably-server/        # main, flag/env wiring
cmd/ably-bench/         # pub/sub load benchmark (exact-once slice)
cmd/ably-loadgen/       # raw-protocol load generator for cluster-scale runs (bench/aws/README.md)
cmd/ably-conductor/     # scenario runner: assigns, ramps, holds, collects, evaluates, reports
cmd/ably-local-sandbox/       # disposable-instance provisioner for SDK test suites (§15)
cmd/compat-gate/        # known-failures gate for the SDK compatibility harnesses
internal/protocol/      # wire types + json/msgpack codec; presence and mutable-message types
internal/auth/          # API-key parsing + Basic auth
internal/realtime/      # WebSocket upgrade, connection loop, attachment cursor, presence, mutation, rewind
internal/rest/          # HTTP handlers + router
internal/core/          # Channel + ChannelManager (live entry list)
internal/storage/       # Storage interface + memory / bbolt / postgres backends
                        #   (the postgres backend carries the cluster bus: pgnotify
                        #   by default, the rebuilt postgres bus, or NATS, §7.2)
internal/serial/        # channelSerial minting + global ordering
internal/id/            # connection IDs, message IDs
internal/compatgate/    # known-failures diff logic behind cmd/compat-gate
internal/loadgen/       # loadgen protocol client, roles, checker, scenarios, conductor logic
```

### 5.1 Channel

A `Channel` is a per-node, per-name structure holding the live message
list. It has **no goroutine of its own**: concurrency is serialised by a
mutex around append. Attachments tail the list at their own pace, with no
fan-out channels and no per-attachment buffering. Each list entry is one
ChannelMessage (§8) — an atomic publish carrying one or more Messages.

Each Channel is paired at construction with its `storage.ChannelStore`
facet (Manager calls `storage.Channel(name, channelAsAppender)` so
the storage holds a back-link to the in-process Channel). Channel
exposes two methods:

- `Publish(ctx, msgs)` — orchestrates a publish: delegates to
  `store.Store(ctx, msgs)`, which mints the channelSerial and
  persists. The link onto the live list arrives via the Appender
  callback — synchronously after commit in memory/bbolt;
  asynchronously via the cluster bus in Postgres (§7.2).
- `Append(cm)` — satisfies the `storage.Appender` contract. It and
  `Discontinuity` are the **only** writers to the linked list, and
  both are called only by the storage backend (never directly by
  publish-path callers, in any mode).
- `Discontinuity(reason)` — satisfies `storage.Discontinuous`: the
  backend could not prove it delivered every cm since the last
  `Append` (§7.2). Under the same lock as `Append` it drops the local
  presence member set (§12.4) and links a marker entry, a serial-only
  cm at the previous tail's serial, so every Stream meets it between
  the cms before and after it (`Stream.Discontinuity`); its attachment
  sends the client a channel update (§4.3).

This is the unified flow: every cm that lands on a Channel's live
list arrives through `storage → Appender.Append`, whether the publish
originated locally or on a remote node.

Each entry holds one ChannelMessage plus wake channels that are closed
once the next entry is linked; parked attachment goroutines wake on that
close. The list is grow-only — older entries become eligible for GC once
no attachment retains a reference (see Memory below).

**Fan-out cost.** On a channel with tens of thousands of attachments on
one node (shape M's hot channel has about 20,000 per node), one Append
wakes that many goroutines at once, and three things decide how long the
last of them takes to queue its frame:

- *Wake channels.* Parking on a Go channel takes the channel's lock, so
  if every attachment parked on one channel per entry, each fan-out would
  end with all of them queueing on that lock to park on the next entry.
  An entry's wake channels are split over slots instead, about 64
  attachments to a slot (a power of two, 1 to 1,024 slots, sized from
  the channel's attachment count when the entry is appended), and each
  attachment parks on the slot its attach-time number selects. A slot's
  channel is made by the first attachment that parks on it, so an entry
  on a one-subscriber channel holds one channel.
- *Wake-up outside the lock.* Append links the entry under the
  Channel's mutex and wakes the parked attachments after releasing it,
  so the wake-up (which readies every parked goroutine, one by one) does
  not hold up `Attach` or the eviction sweep. The list order is fixed
  under the mutex, and an attachment reaches an entry only through its
  predecessor, so it still sees the cms in order.
- *Encode once.* A live frame that is the same for every attachment on
  the channel (a `MESSAGE` with no append delta to resolve, §13.3, or a
  `PRESENCE`) is encoded once per wire format, by the first attachment
  that sends it, and kept on the entry; every other attachment on that
  format queues the same bytes, shared read-only (as the SYNC snapshot's
  frame is, §12.4). A frame that needs a per-attachment transform (an
  append delivered as a delta to one attachment and as the full version
  to another) and every replayed frame (resume and rewind read from the
  log, not the live list) are encoded per attachment. The `echo=false`
  filter (§2.1) only skips a frame, so it does not stop the sharing.

`BenchmarkFanoutEnqueue` (`internal/realtime`) measures one publish to
20,000 attachments, from the publish to every frame queued, on 8 cores
of a laptop: about 41 ms (msgpack) and 36 ms (JSON) on the code before
these changes (the same benchmark run on a copy of the earlier tree), and
about 7 ms for either with them. The socket writes that follow are not
in it.

The first `ATTACH` to a name (or the first publish, or any REST read)
creates the Channel and binds it: `storage.Channel(name, channel)`
registers the Channel as the Appender and Initializes it from the store's
watermark.

**Write-only REST publish.** In cluster mode a REST message publish to a
name with no Channel on this node (so no attachment and no presence
member here) does not bind it. The Manager hands the REST handler an
unbound store (`storage.UnboundPublisher`), and the publish is minted,
written and committed in its batch exactly as through a bound store; the
bus's post-commit hook still announces it, so other nodes' subscribers
receive it (§7.2). Nothing is created on this node: no Channel, no bus
subscription, no sweep entry, nothing for eviction to release 60 s later.
The local fast path is skipped because there is no local appender,
unless the channel was bound here while the publish was in flight, in
which case the fast path (or, on `pgnotify`, the NOTIFY round trip)
delivers it to that binding and the bind's watermark read keeps it
exactly once. A name whose Channel is bound or still binding takes the
normal path; one being evicted takes the write-only path. Presence,
annotations, mutations and realtime publishes keep the normal path: a
presence ENTER, UPDATE or LEAVE always comes through a Channel bound on
this node. A channel can have both: write-only publishes from nodes that
hold no Channel for it, and presence members on nodes that do. The two do
not interact. A write-only publish is a message cm, which changes no
member, so it leaves every node's presence set (§12.4, §12.5) as it was;
the nodes with members receive it on the bus like any other cm. A node
that holds a Channel for the name (an attachment, a presence member, or
a Channel not yet evicted) sends its REST publishes down the normal path,
so its local member set sees them in order with the presence cms.
`ably_channel_unbound_publishes_total` counts the publishes that take
this path.

**Idle-channel eviction.** A node that serves hundreds of thousands of
channels an hour cannot hold every channel it has ever touched. With
`--channel-idle-timeout` set (default 60 s, `0` disables), the Manager
evicts a Channel once all of these hold:

- it has no attachments (no open `Stream`),
- no storage operation on it is in flight (publish, presence, mutation,
  annotation, history, members),
- it has no presence members that this node has seen enter and not yet
  leave (fed by the Appender, so it covers members held in the presence
  grace window after an abrupt disconnect, §12.5, fixture members, and
  members on other nodes whose ENTER reached this node), and
- it has been in that state for the idle timeout.

Eviction drops the Channel (its live list and the storage facet pointer)
and calls `Storage.Release(ctx, name)`, which unbinds the Appender and
frees the backend's per-channel state in this process while keeping all
durable state (§6). The next attach, publish or read binds the channel
afresh: the new Channel is Initialized from the store's current
watermark, so a fresh attach starts at the latest committed serial, and a
resume from an older serial gap-fills from the log (§4.3), including
every message committed while the channel was unbound on this node.

The Manager enforces this without holding a lock across I/O:

- Channel operations and `Attach` pin the Channel (an in-flight count or
  an attachment count) for their duration, so a pinned Channel is never
  evicted. A caller holding a `*Channel` that was evicted after it was
  obtained is redirected: the pin fails, the operation rebinds through
  the Manager, and runs on the fresh Channel. `Attach` on a stale Channel
  returns a Stream on the fresh one (`Stream.Channel()`).
- The sweeper (every `timeout/10`, clamped to 5 ms to 5 s) marks idle
  Channels evicted under the shard lock, then calls `Release` outside
  it. A `GetChannel` for a name being evicted waits until `Release`
  returns and then binds afresh, so a bind never overlaps the `Release`
  of the Channel it replaces. A `GetChannel` for a name that is still
  binding waits for the bind, so no caller sees a half-bound Channel.
- The channel map is split into 64 shards by name hash, so neither
  `GetChannel` (once per publish, attach and REST request) nor a sweep
  holds one lock across all channels.

Three series track it (§10): `ably_channels_bound`,
`ably_channel_evictions_total` and `ably_channel_binds_total`. A bind is
either a first use or a rebind after eviction; the node does not
remember evicted names (that memory is the leak eviction removes), so
the rebind rate is read as the bind rate once the working set is steady.
Go maps keep their buckets after deletes, so each shard map holds about
one slot per peak channel after a churn; everything else a Channel holds
is freed.

With eviction disabled a Channel is held for the life of the process.

**Memory.** Go's GC reclaims entries once no attachment retains a
reference. A slow attachment retains the prefix of the list between its
cursor and the live tail, so memory grows with its lag. The retention
policy (§6) is intended to bound the working set: once a message ages past
the retention window or the per-channel `max_messages` cap, the Channel drops its own
back-pointer to it, so any unreferenced entries become eligible for GC.
Each retained entry also keeps its wake-slot array (8 bytes a slot: 4 KiB
on a channel with 20,000 attachments, at most 8 KiB) and its shared
encoded frames, one per wire format in use, so an attachment lagging on
a large channel holds a few KiB per entry it is behind.

### 5.2 Connection loop

Each WebSocket connection has:

- A read goroutine that decodes inbound frames and dispatches on `Action`.
  It is started by the HTTP handler, which then returns: the socket is
  hijacked, and returning lets net/http free the per-connection read and
  write buffers it would otherwise keep for the connection's lifetime
  (about 8 KiB).
- A write goroutine that drains the bounded outbound queue (below) and
  writes each frame (only one writer per connection per gorilla/websocket
  conventions).
- A heartbeat ticker that sends `HEARTBEAT` if idle.
- An `attachments map[string]*Attachment` keyed by channel name, each
  with its own goroutine tailing the channel's live list.
- A publish worker (below) and a token-expiry goroutine, each started
  only when first needed: on the first publish, and at connect for a
  token credential or on the first inband re-auth. An idle subscriber
  connection therefore runs two goroutines plus one per attachment.

**Outbound queue, write deadline, slow consumers.** Attachments, the
publish worker (`ACK`/`NACK`) and the read loop do not write to the
socket; they encode the frame and push it onto the connection's outbound
queue, which the write goroutine drains. The queue is bounded in bytes of
encoded frames (`--conn-outbound-max-bytes`, default 1 MiB), not in
messages: frames range from a few bytes to the 64 KiB message limit, so
only a byte bound caps the memory one connection can pin. A frame is
always admitted to an empty queue, so a single frame larger than the
bound cannot wedge a connection. The queue is a slice with a head index:
when the consumed prefix passes 32 slots and half the slice, the live tail
is slid to the front, so a connection that never fully drains (one frame
always queued) holds a backing array proportional to its live frames, not
to every frame it has ever been sent.

A push that would take the queue past its bound waits for the writer to
make room (backpressure; this absorbs bursts such as a resume replay),
but for at most `--conn-write-timeout` (default 10 s). Each socket write
carries the same deadline. Either limit being hit means the client is not
reading fast enough, and the connection is disconnected as a slow
consumer: its queued backlog is dropped, a `DISCONNECTED` frame carrying
error `80003` goes out as the last frame when the socket still accepts
it, and the socket is closed. `80003` (connection disconnected) is
retriable: the SDK reconnects and re-attaches each channel from the last
`channelSerial` it received, gap-filling from the log (§4.3). Nothing is
dropped on the server, so `80020` (continuity lost as the delivery rate
was exceeded) would misdescribe it. Other subscribers are unaffected: a
slow attachment waits on its own connection's queue, never on the
channel, and the channel's live list is shared. The
`ably_slow_consumer_disconnects_total{reason}` counter (`queue_full` or
`write_timeout`) records each disconnect (§10).

**Buffers.** The WebSocket read buffer is per connection
(`--ws-read-buffer-size`, default 1 KiB: inbound frames on a subscriber
connection are small, and a larger frame is read in several fills). Write
buffers (`--ws-write-buffer-size`, default 4 KiB) come from a shared pool
and are held only while a frame is being written, so they cost memory per
concurrent write, not per connection.

**Sizing a node for 100k+ connections.** Measured on a laptop (±50%),
50k connections each attached to one channel cost about 9 KiB of heap
and 16 KiB of goroutine stack per connection (three goroutines), about
25 KiB in all; each extra attachment adds one goroutine (about 5 KiB of
stack) plus its attachment state. A lagging client adds up to
`--conn-outbound-max-bytes` while it lags. So 100k connections need about
2.5 GiB before channel and message state. The measurement is the soak
test (`bench/soak/run.sh`), which runs inside a Linux container because
its sockets would exhaust the host's ephemeral ports. Operator settings:

- `GOMEMLIMIT`: set to about 85% of the memory available to the process
  (for example `13GiB` on a 16 GiB instance). The GC then works harder
  as the heap nears the limit instead of letting it double, which is
  what a spike in lagging clients would otherwise do.
- `GOGC`: with `GOMEMLIMIT` set, `GOGC=200` is a reasonable start. The
  per-connection heap is long-lived and per-message garbage is short-lived,
  so a larger `GOGC` spends less CPU on marking the same live set, and the
  memory limit still caps growth.
- `GOMAXPROCS`: leave it at the default. Go 1.25 and later respect a
  container CPU limit, and on a VM it equals the vCPU count.
- File descriptors: one per connection, plus the Postgres pool and bus
  connections. Set the process limit to at least twice the target
  connection count (`LimitNOFILE=1048576` under systemd,
  `--ulimit nofile=1048576:1048576` under Docker).
- Kernel: raise the accept backlog (`net.core.somaxconn` and
  `net.ipv4.tcp_max_syn_backlog` to 4096 or more) so a reconnect storm is
  not dropped at `SYN`, and give load generators a wide
  `net.ipv4.ip_local_port_range`, since each client connection uses one
  local port per destination address and port.

Inbound `MESSAGE` / `PRESENCE` is validated on the read goroutine
(clientId resolution, connectionId stamping, presence attachment/mode
checks) and then handed to a per-connection **publish worker** — a single
goroutine draining a FIFO queue of publish tasks — so the storage write
happens off the read goroutine and never blocks decoding of the next
frame. The worker calls `channel.Publish(ctx, msgs)` (or `Mutate` /
`PublishPresence`), which returns once storage has committed; the link
onto the local linked list happens asynchronously via the Appender
callback the storage holds (synchronous in memory/bbolt, NOTIFY-driven in
Postgres — see §7). Only then does the worker emit the frame's `ACK` (or
`NACK` on failure), echoing the publish `msgSerial`.

A single FIFO worker per connection is deliberate: it keeps this
connection's channel appends in publish order and emits `ACK` / `NACK` in
`msgSerial` order (the SDK correlates acknowledgements positionally, so an
out-of-order or premature ack corrupts its pending-publish accounting).
Validation rejections are enqueued through the same worker so their `NACK`
stays ordered behind any still-in-flight publishes.

An inbound `MESSAGE` / `PRESENCE` `msgSerial` must be monotonic per
connection. A frame whose `msgSerial` repeats or goes backward is a client
retransmit — e.g. the SDK re-flushing a queued publish with the same
`msgSerial` after a reconnect (RTL6c2) — and is **dropped** on the read
goroutine before it reaches the worker: it is neither re-published nor
re-`ACK`ed. A duplicate `ACK` for a `msgSerial` the SDK has already dequeued
would corrupt its positional accounting. A forward skip is accepted.

Inbound `ATTACH` / `DETACH` are handled by the connection itself on the
read goroutine, calling into `ChannelManager` to get/release a Channel.

## 6. Storage

> **Retention.** The Postgres backend (cluster mode) enforces retention:
> a channel keeps its log for the continuity window (`--message-retention`,
> default 2 minutes) unless its namespace is persisted, in which case it
> keeps it for `--persisted-retention` (default 24 hours). The mechanism is
> partition drop, described in §6.3. The memory and disk backends do not
> yet enforce retention. References elsewhere to "the retention window" or
> messages "aging out" mean the channel's retention class in §6.3.

The storage interface has two facets: a process-wide `Storage` that
hands out per-channel `ChannelStore`s and owns any shared resources
(e.g. a bolt DB handle, a pgxpool), and a per-channel `ChannelStore`
exposing two operations:

- `Store(ctx, msgs)` — mints a `channelSerial` and persists the resulting
  ChannelMessage atomically, then returns it. For a `create` it stamps
  each `Message.serial = "<channelSerial>:<idx>"` (serial == version);
  for an `update`/`delete`/`append` the `serial` is the caller-supplied
  target and only `version` takes the new `<channelSerial>:<idx>` (§13.1).
  If any contained `Message.id` was already seen on this channel within
  the retention window, the call is idempotent: the originally-persisted
  ChannelMessage is returned with `idempotent=true` and no new row is
  written.
- `History(ctx, query)` — bounded forward range scan ordered by
  channelSerial; backs both the REST history endpoint and attachment
  resume gap-fills (§4.3). Messages and presence share one stream and
  one channelSerial namespace (§12.1), so the query carries a kind
  selector: a message-history scan skips presence cms and vice versa.
- `StorePresence(ctx, presence)` — the presence analogue of `Store`
  (§12.2): mints a channelSerial, stamps each `PresenceMessage.serial`,
  persists the presence cm onto the same stream, and in the same atomic
  step folds it into the channel's **membership set** (ENTER/UPDATE
  upsert the member keyed by `connectionId:clientId`, LEAVE removes it).
  The Appender then delivers the cm exactly as for a message publish.
- `Members(ctx)` — returns the current membership set plus the
  channelSerial it is current as-of; backs presence sync (§12.4) and the
  REST `GET .../presence` endpoint. The set is owned by the backend
  (§12.5): a map in memory, in-memory in bbolt, a `presence` table in
  Postgres.
- `Store` also handles **mutations** (§13): an `update`/`delete`/`append`
  is a publish whose Message carries an `action` and a target `serial`.
  The backend validates the target exists within retention, applies the
  shallow-mixin merge against the message's current latest version,
  persists the new version as an ordinary cm on the log, and updates two
  derived structures it owns — a **serial → versions** secondary index
  (backing version-history reads and target validation) and a
  **latest-version projection** per message serial (backing the collapsed
  `GET .../messages` and `GET .../messages/{serial}`). The projection is
  the message-side analogue of the presence membership set: a map in
  memory, a `messages` bucket in bbolt, a materialised `messages` table in
  Postgres; the versions index is a `versions` bucket / partial index over
  the log (§6.3).
- `History` therefore has two message modes: the default collapses to the
  latest version of each message positioned at its create serial; a
  by-serial version scan returns every version of one message ordered by
  `version` (§13.4).

`Storage.Release(ctx, name)` is the storage half of idle-channel
eviction (§5.1). It unbinds the channel's Appender (once it returns, the
old Appender is never called again) and frees the per-channel state the
backend holds in this process, keeping every durable thing: log,
serials, idempotency keys, presence set, projections. A later
`Channel(name, appender)` binds a fresh Appender and Initializes it from
the store's watermark. Per backend: memory keeps its channelStore (it is
the only copy of the data) and drops only the binding; bbolt drops the
channelStore unless it still holds presence members (its presence set is
in memory only), in which case it keeps it unbound; Postgres drops the
channelStore and its LISTEN dispatch entry (the shared LISTEN connection
listens on one broker channel, so there is nothing to UNLISTEN per Ably
channel).

Each `ChannelStore` is created with an `Appender` callback —
`Storage.Channel(name, appender) ChannelStore`. The Appender is the
single delivery path for committed cms: the backend invokes
`appender.Append(cm)` on every fresh publish (skipped on idempotent
returns, where the original was delivered when first persisted).
Memory and bbolt fire it synchronously after commit. Postgres fires
it asynchronously through the cluster bus (§7.2): with `pgnotify` from
the LISTEN goroutine after the NOTIFY emitted inside the commit tx
round-trips, including for the publisher's own publish; with `postgres`
or `nats` the publishing node's own cm takes a fast path straight after
commit and other nodes' cms arrive over the bus.

Serial minting and idempotency live behind this interface so persistent
backends (bbolt, Postgres) can restore monotonic generator state across
restarts alongside the data it secures (§8). The canonical Go
signatures live in `internal/storage/storage.go`.

### 6.1 Memory backend

Per-channel ring buffer for messages, bounded by count *and* age.

### 6.2 Disk backend

[bbolt](https://github.com/etcd-io/bbolt) — a pure-Go embedded B+tree
KV store with crash-safe writes. Single file at `--data-dir/ably.db`.
Chosen because the disk backend's job is narrow ("survive crashes for
a single process") and bbolt gives us that without coupling the disk
layer's schema to the Postgres cluster backend.

Layout — channel-scoped buckets via composite keys:

- `channel_messages`: the append-only log, keyed `<channel>\0<channelSerial>`
  (see §8 — the atomic-publish identifier `<timestamp>-<counter>@<seriesId>`).
  Values are the msgpack-encoded `protocol.ChannelMessage` blob (a
  message or presence cm). bbolt's byte-order iteration over a
  `<channel>\0` prefix yields a channel's ChannelMessages in publish
  order, mirroring the Postgres backend's PK range scan.
- `ids`: keyed `<channel>\0<Message.id>`, value is the channelSerial
  the id landed in. bbolt has no secondary indexes, so this is the
  manual equivalent of Postgres's idempotency index.
  Entries are dropped by the same sweep that trims `channel_messages`
  past TTL — idempotency is bounded by message retention.
- `versions` and `messages` (mutable messages, §13): the bbolt analogue
  of Postgres's `channel_messages_serial_idx` and the materialised
  `messages` table. `versions` is keyed
  `<channel>\0<message_serial>\0<version_serial>` so a prefix scan
  enumerates a message's versions in order; `messages` is keyed
  `<channel>\0<message_serial>` and holds the merged latest version.
  Both are written by the same single writer as `channel_messages` and
  trimmed by the same retention sweep. (Unlike the presence membership
  set, these are durable — they are message state, not connection-scoped.)

Per-process `seriesId` is regenerated on every `Open` and generator
monotonic state is not persisted. The §8 serial format makes
post-restart monotonicity fall out naturally: the 14-character
zero-padded ms timestamp is the leading lex-comparison key, and wall
clock advances between restarts, so a post-restart Mint sorts after
all prior serials. The same-millisecond restart with an unlucky new
seriesId is the only edge case we don't guarantee, and we don't.

Retention is enforced by a background sweep goroutine that, per
channel, walks the ordered `channel_messages` keys from oldest forward and
deletes anything past the message TTL or beyond the per-channel cap
(the cap counts ChannelMessages, since each is the unit of an atomic
publish). Because keys are serial-ordered and writes are append-only,
the sweep stops at the first non-expired key per channel.

bbolt has no native TTL, no secondary indexes, and a single-writer
model — all of which suit this use case: short-lived data, one writer
per node (the publish path), and the only read pattern beyond the
live tail is a bounded history range scan.

### 6.3 Database backend (cluster mode)

Postgres only, version 14 or later. With the `pgnotify` or `postgres` bus (§7.2), LISTEN/NOTIFY
gives us pub/sub and the same database serves as the durable store, so
cluster mode needs nothing beyond a single Postgres. The `nats` bus adds
a NATS server or cluster for delivery; Postgres stays the store.

Schema. The shipped DDL is the migration files under
`internal/storage/postgres/migrations/` (`0001_initial.sql` to
`0004_presence_nodes.sql`); they are the source of truth and each carries
the reasoning in its header. In outline (columns elided where the
migrations say more):

- `channels (name PK, channel_serial, initial_channel_serial)`: one row
  per channel name, holding the serial the next publish continues from
  (minted under the row lock by `advance_channel_serial` and, batched, by
  `publish_batch_lock`) and the immutable seed serial a rewind to the
  channel's beginning attaches at (§4.3). `ensure_channel` creates a row
  with a fresh seed. Rows are pruned once idle (see "Channel rows"
  below).
- `channel_messages`, the append-only LOG: one row per individual
  `Message`, `PresenceMessage` or annotation (`kind`), all interleaved in
  one `channel_serial` namespace (§12.1), keyed `(channel, channel_serial,
  idx, persisted)`. `idx` is the position within the publish; `id` is the
  client-supplied idempotency key (nullable); `message_serial` is the
  message identity a row is a version of (NULL for presence); `is_append`
  marks a streamed append (§13.3); `summary` holds the annotation summary
  snapshot of an annotation row (§14.2); `payload` is the msgpack
  `Message` or `PresenceMessage`. The action is in the payload, not a
  column. Partitioned on two levels (see "Retention"). Indexes:
  `channel_messages_id_idx (channel, id) WHERE id IS NOT NULL`, a plain
  index because a partitioned table cannot carry a unique index without
  its partition key (the `channels` row lock is the arbiter of id
  uniqueness); and `channel_messages_versions_idx (channel, message_serial,
  channel_serial, idx) WHERE message_serial IS NOT NULL`, which backs
  version scans and the update/delete target lookup (§13.4).
- `messages (channel, message_serial, payload, deleted, persisted)`, keyed
  `(channel, message_serial, persisted)`: the latest-version projection,
  one row per message, upserted in the same transaction as the version's
  log row. A delete sets `deleted`; the row and its versions stay
  queryable (§13.2). Partitioned like the log, ranged on `message_serial`
  (the create's serial, which is also the order of collapsed history).
- `presence (channel, connection_id, client_id, channel_serial, payload,
  node_id, expires_at)`, keyed `(channel, connection_id, client_id)`: one
  row per live member, maintained in the same transaction as the presence
  cm (§12.5); `presence_node_idx (node_id)` serves the reaper.
  `presence_nodes (node_id PK, expires_at)` is the per-node liveness
  lease.
- `retention_state (key, value)`: the legacy-leaf bound recorded by
  migration 0002 (below). `schema_migrations (version PK, applied_at)`:
  the migration tracker.
- SQL functions: `format_channel_serial`, `next_channel_serial`,
  `ensure_channel`, `advance_channel_serial` (0001) and
  `publish_batch_lock` (0003).

Reads over the log (`channel_messages`) add `kind = 'message'` (or
`'presence'`) to the predicates. Collapsed message history reads the
materialised `messages` table ordered by `message_serial`; a version scan
reads `channel_messages` via `channel_messages_versions_idx`. Retention
does not delete log rows: it drops whole leaf partitions (below), and a
message's projection row goes with the leaf that holds its create serial.
The `presence` projection is independent of the log's retention sweep
(§12.5).

The DDL ships as versioned migrations under
`internal/storage/postgres/migrations/*.sql` (e.g. `0001_initial.sql`)
and is applied at `postgres.Open` by an auto-migrate sweep:

1. Acquire a session-scoped `pg_advisory_lock` on a fixed int8 key
   (arbitrary — advisory-lock keyspace is per-database and opt-in).
   N nodes booting simultaneously block here; only one applies the
   migrations, the rest observe an up-to-date state and skip.
2. `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT
   PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())` —
   the migration tracker itself.
3. Read the applied version set; for each embedded migration whose
   version is not in the set, run its SQL plus the
   `schema_migrations` insert in a single transaction. Failure
   rolls back the migration *and* the tracker row, so the next Open
   retries the same migration.
4. Release the advisory lock.

The mechanism is forward-only, hand-rolled (no migration library),
and matches what a load balancer rolling-restart of N nodes against
the same Postgres needs: every restart is a no-op except the one
that introduces a new migration file.

Every migration runs under `SET LOCAL lock_timeout = '5s'`, so a lock
held by a node still serving traffic ends the attempt with `55P03` after
5 seconds instead of waiting without limit (a waiting `ACCESS EXCLUSIVE`
request also queues every later statement on the table behind it). The
attempt rolls back whole, and the runner retries a `55P03` or a deadlock
(`40P01`) up to 5 attempts in all, 250 ms apart. A migration whose lock
never frees therefore fails `Open` after about 26 seconds and changes
nothing; the timeout bounds the wait for a lock, not the time a
migration holds the locks it did get.

**`0002_partitioned_log` is the one migration that cannot run under
traffic**, and the only one that needs an offline step; the others
(`0001`, `0003`, `0004`) take brief locks or only add objects. It takes
`ACCESS EXCLUSIVE` on both log tables for the whole transaction, so every
node, old or new, blocks on every publish until it ends. On a populated
log it attaches the old table as one leaf, which validates every row
against the partition bound and builds the new primary key on it, and
does the same for the `messages` projection: time in proportion to the
number of rows. The rate (rows per second) depends on hardware, row size
and the existing indexes, and has not been measured; see §11 "Upgrading
across 0002" for the procedure and how to estimate it. It also changes
what the old version writes to: until every node runs the new version,
old nodes write channels of persisted namespaces into the live class,
where those rows age out on the continuity window. Rows written before
the upgrade also land in the live class, so a persisted namespace's
history from before the upgrade is kept only for the continuity window.

Per-publish writes run inside one transaction that first locks the
channel's row in `channels` (`advance_channel_serial` mints the next
serial under that row lock) and only then checks the idempotency index.
Concurrent writers therefore serialise per channel without contending
across channels, and two concurrent publishes carrying the same id
cannot both pass the check: the second waits for the row lock, and its
next statement sees the first's committed rows. A duplicate returns the
original cm and rolls the transaction back, so it does not burn a serial. Each node generates its own seriesId at process start; the
serial format itself (the `@seriesId` suffix) disambiguates concurrent
mints, so generator state is not shared across nodes.

**Retention.** The two tables that grow with every publish,
`channel_messages` and `messages`, are partitioned so that retention is a
partition drop, not a row-by-row DELETE:

- **Level 1, `LIST (persisted)`: the retention class.** `persisted = FALSE`
  (the `_live` partitions) holds channels outside any persisted namespace;
  they keep the continuity window, `--message-retention`, default 2
  minutes, which is the period a client can resume across (§4.3).
  `persisted = TRUE` (the `_persisted` partitions) holds channels whose
  namespace is a `[[namespaces]]` entry with `persisted = true` (§9); they
  keep `--persisted-retention`, default 24 hours. A channel's namespace is
  the part of its name before the first `:`.
- **Level 2, `RANGE` on the serial: time.** `channel_messages` is ranged on
  `channel_serial` and `messages` on `message_serial` (the create's
  serial). A serial starts with its 14-digit zero-padded mint time in ms
  (§8), so a serial range is a time range and the bounds are plain digit
  strings, for example `FROM ('01727700000000') TO ('01727700060000')`.
  Each leaf covers one width: half the class's retention, clamped to
  between 1 minute and 1 hour (1 minute for the continuity window, 1 hour
  for 24 hours). While sweeps succeed, a row lives for at most its
  retention plus one width plus two maintenance ticks (drops run on every
  second tick).

Every node runs a maintenance sweep at `Open` (before it serves) and then
a tick every half leaf width (30 s by default). It reads the time from the
database's clock, the same clock that mints serials, so a node with a
skewed clock cannot drop a leaf still being written. Ticks alternate:
even ticks run step 1 and odd ticks run step 2, never both in one tick
(`Open` runs step 1 only). A detach can wait up to 30 seconds holding the
parent's `SHARE UPDATE EXCLUSIVE` lock, which creation's `ATTACH` also
needs, under a 10 second lock timeout; back to back, a slow detach could
fail the creation that publishes depend on. With the hour of lookahead,
creating on every second tick (a minute by default) loses nothing, and a
leaf lives one more tick at most.

1. **Create ahead.** Under a transaction-scoped advisory lock, it creates
   every missing leaf from the current slot to at least an hour (and at
   least two widths) ahead, in both classes of both tables, filling any
   gap. A leaf is created as a standalone table and then attached, which
   takes only a `SHARE UPDATE EXCLUSIVE` lock on the parent, so creation
   does not block publishes. A publish whose serial had no leaf would
   fail, so a failure here fails `Open`; in the loop it is logged and
   retried, and the hour of lookahead is the margin for sweeps failing.
2. **Drop expired.** For every leaf whose whole range ends at or before
   now minus its class's retention, it runs `ALTER TABLE ... DETACH
   PARTITION ... CONCURRENTLY` and then `DROP TABLE`. The concurrent detach
   waits for older snapshots instead of locking out new ones, so a drop
   does not stall publishes; each detach is bounded to 30 seconds, and one
   cut short is finished with `DETACH ... FINALIZE` on a later sweep. A
   detached leaf whose `DROP` failed is found again by name and dropped.
   Only one node drops at a time (a session advisory lock, tried rather
   than waited for), and the drop lock is separate from the creation
   lock, so a slow drop never holds up creation. `Open` creates but does
   not drop. Leaves are dropped oldest first, and a class's drop stops at
   the first leaf that will not detach, so the log only ever loses a
   channel's oldest cms: a catch-up relies on that to prove its
   continuity (§7.2).

   The `DROP TABLE` needs `ACCESS EXCLUSIVE` on the detached leaf, which
   queues behind any reader still holding it and makes every later reader
   queue behind the drop. It therefore runs in its own transaction under
   `lock_timeout = 1s`. On timeout the leaf stays as a detached orphan
   (counted in `ably_storage_partition_drop_lock_timeouts_total`, not
   retried in the same sweep) and the next drop tick's by-name pass drops
   it. The chain's log range reads (gap fill, catch-up, reconcile, §7.2)
   are bounded below by the channel's retention floor (the same bound a
   resume is checked against), and run as unnamed statements planned for
   the actual parameters, so the planner prunes the leaves older than
   retention and the reads never lock a leaf a drop is waiting on, except that a checked catch-up (§7.2) also asks whether the cm at the mark still exists, which touches the one leaf that would hold it. A
   cached statement would switch to a generic plan after five executions,
   and a generic plan locks every leaf. A cm older than the floor is
   outside the retention the channel promises; a gap fill that would have
   reached below it does not.

A resume whose cursor was minted before now minus the channel's retention
is refused as a discontinuity (§4.3). "Now" is the node's clock corrected
by its offset from the database clock (measured at every sweep) plus one
second, so small clock errors err towards refusing. For a persisted
channel the floor is also at least the bound of the legacy leaf a
migrated log became (recorded in `retention_state`), because those rows
sit in the live class.

The idempotency lookup is bounded to serials from the channel's
retention floor up to the serial being written (every stored cm of the
channel sorts below it), so it reads only the leaves in that range and a
channel is idempotent within its own retention. It spans both classes, so
rows left in the other class by the migration or by a namespace change
still dedupe. A mutation, annotation or version read
whose target has aged out finds no row and gets `ErrTargetNotFound` (§13.2):
the projection row goes with the leaf that holds the message's create
serial. The `presence` table and the `channels` table are not partitioned:
presence rows are bounded by live membership (§12.5), and a `channels` row
is one small row per channel name, pruned once the channel has been idle
past every retention (see "Channel rows" below).

Every node must run with the same retention settings, since whichever node
sweeps applies its own. Changing a channel's class (editing a namespace's
`persisted` flag) applies to new writes; rows already written age out with
the class they were written in.

A database created before migration `0002_partitioned_log` keeps its rows:
a non-empty old table becomes one leaf of the live class covering
everything up to a minute past the migration, and ages out on the
continuity window.

Series (§10): `ably_storage_partitions_created_total{table}`,
`ably_storage_partitions_dropped_total{table}`,
`ably_storage_retention_errors_total`,
`ably_storage_partition_drop_lock_timeouts_total`, and the gauges
`ably_storage_log_bytes{table}` (heap, indexes and TOAST of every leaf) and
`ably_storage_log_partitions{table}`, as of the node's last sweep.

**Publish batching.** A message publish (`Store`, from REST or realtime
alike) can be committed together with other publishes of the same node in
one transaction. This is how one Postgres primary carries tens of
thousands of writes a second: the cost of a commit is shared by every
publish in it. It is on by default (`--publish-lanes=4`) with every bus
(§7.2); `--publish-lanes=0` commits every publish in its own transaction.
A presence operation (`StorePresence`, §12.2) joins the same lanes and
batches, so ENTERs and LEAVEs from
many rooms share one commit and a room's row lock is taken once per
batch, not once per operation (§12.5). Mutations and annotations are not
batched.

The policy is leading edge, not a fixed window. A node has
`--publish-lanes` lanes (default 4); a channel's name hashes
to one lane, so all of a node's publishes to one channel share a lane and
keep their order. Per lane:

1. If nothing is in flight, the first publish commits at once. At low
   load a publish costs exactly one commit.
2. While a commit is in flight, arriving publishes queue. When it
   returns, everything queued (up to `--publish-batch-max`, default 200)
   commits as the next batch. At high load the batch grows to match the
   commit latency, so the node tunes itself: 10 nodes at 52,000 writes/s
   with a 3 ms commit is about 16 publishes a batch, and at 104,000/s
   about 31; Postgres sees about 3,000 commits/s either way.
3. A channel is in at most one in-flight batch. If a batch has been in
   flight longer than `--publish-linger-max` (default 5 ms), the queued
   publishes of other channels start a second batch, so a stalled commit
   delays only the channels in it (at most two batches per lane).
4. The queue is bounded (`--publish-queue-max`, default 10,000 per lane).
   Beyond it a publish is refused at once with Ably error **42910** (HTTP
   429 on REST, a NACK on realtime): nothing is stored and the client
   should back off and retry. Presence the server synthesises is exempt
   from that bound but has its own, 8 x `--publish-queue-max` of it per
   lane (below, "Presence in a batch").

Inside a batch, in two round trips (migration `0003_publish_batch`):

- **Round trip 1**, `BEGIN` plus `publish_batch_lock`: the channels rows
  of the batch are locked in sorted name order. A row already locked by
  another transaction (a hot channel being written from another node) is
  skipped (`FOR UPDATE SKIP LOCKED`) and that channel's publishes are
  deferred to the head of the next batch, so cold channels' ACKs are not
  delayed by it. A publish deferred twice waits for the lock in its next
  batch, so a hot channel cannot be starved. Waited-for rows are locked
  first, in one sorted statement, and everything after it only skips,
  so batches do not deadlock on channel rows. Client ids
  are then checked under the locks in one set-based lookup, and one
  channelSerial is minted per publish in queue order.
- **Round trip 2**: one multi-row `INSERT` into each of `channel_messages`
  and `messages` (message publishes only), the presence operations of the
  batch folded into the `presence` table (below), the bus's
  in-transaction hook for every cm (queued into
  the same round trip: one `pg_notify` per cm for `pgnotify` and
  transactional `postgres`, nothing for coalesced `postgres` and `nats`),
  and `COMMIT`. Statements are prepared (pgx caches them). After the
  commit the bus's post-commit hook runs for each cm in batch order (the
  publisher fast path, the NATS publish, the coalesced wake-up mark), and
  each cm names the serial before it on its channel, counting earlier cms
  of the same batch, so the chaining buses (§7.2) see exactly the
  predecessor chain a single publish would give them.
- Everything that can reject a single publish (the id format; an id or
  channel name that is not valid UTF-8 or contains NUL, which Postgres
  cannot store, refused with 40031 or 40010) is checked before it is
  queued, so one bad publish cannot fail a batch. A publish that repeats a client id of an earlier publish
  of the same channel in the batch takes that publish's result,
  idempotently. Server-generated ids are unique by construction and skip
  the lookup on a first attempt.
- Each commit attempt is bounded (15 s), so a stuck connection cannot
  wedge a lane. A batch that fails is retried once. The first attempt's
  `COMMIT` may have reached the database with only its reply lost, so
  the retry looks up every publish's id, server-generated ones included.
  A publish found stored under the serial the first attempt gave it is
  that attempt's own commit: it is returned as a fresh publish and the
  bus's post-commit hook, which the lost attempt never ran, runs now, so
  delivery is not left to the sweep. If the retry fails too, every
  publish in the batch is refused with Ably error **50003** (HTTP 503, or
  a NACK). A client retry with the same client-supplied id is then
  deduplicated; a publish that carried no id may, rarely, be stored twice
  if the client retries one whose COMMIT did land. Publishes caught by
  shutdown get the same error.
- ACKs are per publish, sent when its batch commits.
- **Presence in a batch.** Each presence publish is a cm like a message
  publish: it gets one channelSerial in queue order, names its
  predecessor for the chaining buses, and its ids are checked for
  idempotency (a genuine presence op always carries its server-stamped
  id; a synthesised LEAVE has none and is not checked). Its operations
  are folded in batch order to one final state per member key, last
  writer wins: an ENTER then a LEAVE of one member in the same batch
  removes it, a LEAVE then an ENTER keeps it with the ENTER's data. The
  fold is then one `DELETE` for the members that left and one
  `INSERT ... ON CONFLICT DO UPDATE` for the rest (owning node, or the
  fixture sentinel, and an `'infinity'` lease, §12.5). Every
  writer of a room's `presence` rows holds the room's channels row lock
  first, so two batches never contend on them; the reaper, which does
  not, skips rows locked by a writer (`FOR UPDATE SKIP LOCKED`), so it
  cannot deadlock with a batch, and the lease bump writes no member row.
  A retried batch re-checks presence ids like message ids. A synthesised
  LEAVE or a fixture ENTER has no id, so it may be stored twice if the
  first attempt's COMMIT landed: a second LEAVE for a member already
  gone, or a second ENTER that upserts the same row. Presence the server
  synthesises (a teardown or grace LEAVE, a fixture member) is queued
  even past `--publish-queue-max`, and still
  committed if its bounded caller stops waiting first: nothing retries
  it, and a dropped LEAVE would leave its member behind (for as long as
  its node lives). It has a bound of its own, 8 x
  `--publish-queue-max` of it queued per lane, so a commit that stalls
  during a mass disconnect cannot grow the queue without limit. Beyond
  that bound it is not refused but written in a transaction of its own,
  as with `--publish-lanes=0`, once the publishes of its channel
  that were queued or in flight on the lane when it was turned away have
  completed (so it never overtakes an earlier operation on the channel,
  such as the ENTER of the member it removes), and once one of the
  `--presence-max-inflight` slots is free (so the overflow holds at most
  that many pool connections); each counts in
  `ably_publish_server_presence_unbatched_total`. If its caller's deadline
  ends before both, it is queued after those publishes after all, past the
  bound, and commits when they do
  (`ably_publish_server_presence_forced_total`): the bound gives way only
  for a channel whose earlier publishes are themselves stuck. A
  publish on the same channel queued after it was turned away can still
  commit before it; for a teardown or grace LEAVE that is another member's
  operation, since the departed connection writes nothing more. (The
  reaper's LEAVEs and lease-lapse re-entries never join a batch, §12.5.)

**Channel rows.** A channel's `channels` row (its serial and initial
serial) is created by the first write or bind that needs it. A batched
publish on a channel with no row, a message publish or a batched presence
operation alike, creates it inside `publish_batch_lock`,
in round trip 1: the missing rows are inserted in sorted name order
(`ON CONFLICT DO NOTHING`) and then locked without waiting, so a cold
channel's first publish costs no round trip of its own. This matters for
write-only REST publishes (§5.1), which reach cold channels without a
bind. The insert can wait on another transaction that is inserting the
same new name uncommitted (two nodes' first publishes to one channel at
the same instant): at most one commit, and never a deadlock, since every
transaction inserts new rows in sorted order and waits for nothing else
after them. Each node keeps a bounded set of names it knows have a row
(65,536 per database, oldest forgotten first): a bind of a known name
reads the row with a plain `SELECT` instead of `ensure_channel`, which
writes a new row version and waits for the row lock of a channel another
node is publishing on. A known name whose row has since been pruned (see
below) reads no row, and the bind falls back to `ensure_channel`, which
creates it. Either read comes after
the bus subscription, so the bind misses nothing (§7.2).

**Pruning `channels` rows.** One row per channel name ever used is
unbounded: a workload that touches millions of distinct names an hour
(the 1x shape in `bench/aws` touches 2.2 million) would leave that many
rows behind, each probed by the primary key on every batch. Each drop tick
of the retention sweep (§6.3 "Retention", step 2), under the drop lock,
deletes one chunk of up to 1,000 rows whose `channel_serial` was minted
longer ago than the longest retention (`--persisted-retention`, default 24
hours; the larger of the two if configured the other way round) and that
have no `presence` row, using `FOR UPDATE SKIP LOCKED` so a publish in
flight keeps its row. It counts them in
`ably_storage_channel_rows_dropped_total`. At most one chunk per node per
drop tick bounds the load the step adds, and also bounds the pruning rate:
with the default 30 s maintenance interval a node's drop ticks are a minute
apart, so one node prunes at most 1,000 rows a minute, 60,000 an hour. The
drop lock is held by one node at a time, so the fleet prunes at most one
chunk per drop tick of any node. A workload that creates more than that
many new names an hour grows the table until the fleet is large enough or
the maintenance interval short enough to keep up; the growth rate is the
difference. There is no index on `channels.channel_serial` (building one
is a blocking operation on a hot table), so a tick that finds nothing to
prune scans the table once.

Consequences of deleting a row, none of which loses a message, since by
the predicate no cm of the channel is older than the longest retention
and so none survives:

- A later publish recreates the row through `publish_batch_lock` (or
  `ensure_channel`) with a fresh time-based seed serial. That serial is
  above every old serial of the channel, because the old row's serial was
  already older than the retention floor when it was deleted.
- `initial_channel_serial`, the seed a rewind to the channel's beginning
  attaches at, is lost: after the row is recreated the beginning of the
  channel is the new seed (§4.3). The old beginning could not be served
  anyway, as every cm after it has aged out.
- A node that has the channel bound keeps its delivery mark at the old
  serial. The first publish after the idle period announces the new seed
  as its predecessor, which is ahead of the mark, so the delivery point
  holds it and the gap fill reads it from the log about 100 ms later
  (§7.2); that one cm is delivered late, none is lost, and no
  discontinuity is signalled because the log read returns the cm.
- A resume with a `channelSerial` from before the idle period is already
  refused as a discontinuity by the retention floor (§4.3).

What it costs: a publish's latency floor is still one commit before its
ACK (plus the wait for the in-flight batch, at most about one commit
under load). One hot channel is still serialised by its row lock: across
nodes it is one commit per batch, and many publishes from one node share a
lock hold. Per-channel batches are shallow (52,000 writes/s over 700,000
channels is about one message per channel per batch), so the gain is
width across channels, not depth per channel. Cluster write throughput is
still one primary's per database; channel sharding (§6.4) spreads
channels over several.

Series (§10): `ably_publish_lanes` and `ably_publish_linger_max_seconds`
(gauges: the configuration in effect),
`ably_publish_batch_size` (histogram),
`ably_publish_commits_total`, `ably_publish_commit_seconds` (histogram),
`ably_publish_lane_queue_depth{lane}`, `ably_publish_deferred_total`,
`ably_publish_batch_retries_total` and
`ably_publish_nacks_total{reason}` (`queue_full`, `commit_failed`, and
`presence_inflight` for a presence write refused over its in-flight
bound with `--publish-lanes=0`, §12.5), `ably_publish_server_presence_unbatched_total`
(server-synthesised presence written outside a full lane, above) and
`ably_publish_server_presence_forced_total` (queued past the bound
because its channel's earlier publishes were stuck), `ably_storage_channel_rows_dropped_total` and
`ably_storage_channel_binds_total{source}` (`ensure`: a bind that ran
`ensure_channel`; `read`: a bind that read a row the node knew existed).

### 6.4 Channel sharding

One Postgres primary bounds cluster write throughput (§6.3). To go past
it, `--postgres-dsn` takes a comma-separated list of databases, and each
channel is stored in exactly one of them.

**Why this is small.** Nothing in the data model spans channels. A
channel's serials, log, idempotency keys, presence set, message
projection, versions and annotation summaries are all keyed by the
channel, and every write and read names one channel. So a hash of the
channel name to a database is a routing rule with no cross-shard
invariant: no transaction spans two databases, and no node owns a
channel. Postgres stays the sequencer (§8): the channel's row lock in its
shard orders it.

**The list.** Entries are URL-form DSNs (`postgres://` or
`postgresql://`), separated by commas. A comma separates two entries only
when a URL starts straight after it, so a single DSN with commas of its
own (a multi-host URL, or a key=value DSN) stays one DSN; a list of two
or more therefore needs URL-form DSNs. An empty entry or a repeated DSN
is refused at startup, as is a value that looks like a list of key=value
DSNs. The shard count is the list length. One DSN behaves as without
sharding: the server opens one database as before, with no routing layer
between the channel and its store; the only additions are one read at
startup (below) and the `ably_storage_shards` gauge.

**The hash.** Shard = jump consistent hash (Lamping and Veach, 2014) of
a 64-bit key: FNV-1a over the channel name's bytes, mixed by the
SplitMix64 finalizer. It depends only on the name and the shard count,
so every node given the same list routes a channel to the same database.
Jump hash spreads channels evenly and, if the count grew from n to n+1,
would move only the channels that land on the new shard (about 1/(n+1)).
The mixing keeps the shard choice independent of the publish lane choice
(FNV-1a mod lanes, §6.3), so each shard's channels still spread over a
node's lanes. `postgres.ShardFor` is the function; a test pins its
output.

**One full store per shard.** Each shard is a complete Postgres backend
of its own: its own pool, migrations, partition maintenance and
retention sweep (§6.3), presence lease and reaper (§12.5), publish lanes
(§6.3), and cross-node bus connection (§7.2). What a single-database node
does once, a sharded node does once per shard, against that shard only.

| Operation | Scope | How it is handled |
|---|---|---|
| Publish, mutation, annotation, presence write | one channel | routed to the channel's shard; a publish batch never spans shards (lanes are per shard) |
| History, versions, annotations, members, resume floor | one channel | routed |
| Serial minting, idempotency lookup | one channel | routed (the `channels` row lock and the id index are in the shard) |
| Bind, release (idle-channel eviction, §5.1) | one channel | routed |
| Migrations, partition creation, retention drop | account-wide | per shard, at open and in each shard's sweep |
| Presence lease bump and dead-node reaper | account-wide | per shard, with one node id and a lease row in every shard's `presence_nodes`; a reaped member's LEAVE is published on that shard |
| Bus watermark sweep and reconnect reconcile (the `channels` scans, §7.2) | account-wide | per shard, over the node's channels bound on that shard; at most 4 batched catch-up queries in flight per node, over all its shards |
| `pgnotify` LISTEN, `postgres` bus LISTENs | account-wide | per shard: a channel's NOTIFY is sent and heard on its own shard |
| `nats` bus | per channel | unchanged; one NATS connection per shard, and a channel's subject is only published and subscribed by its shard |
| `/readyz` | account-wide | ready while shard 0 and a majority of the shards are reachable, each with its pool, its bus and its publish lanes (§11); the error names the shard; see "A shard that is down" below |
| `ably_bus_*` series | account-wide | summed over shards; connected only while every shard's bus is |
| `ably_storage_*`, `ably_publish_*` series | account-wide | one set per shard, labelled `shard` |
| `GET /stats` stub | account-wide | touches no storage |
| Sandbox per-app schemas (§15) | account-wide | created in and dropped from every shard |

**Shard identity.** A list is fixed for the life of its data, and the
server checks this at startup. Shard 0 opens first. On its first open
each shard's database records, in a `shard_identity` table, its index,
the list length, a list id (minted by shard 0 and carried by every other
shard) and a random id of its own; once every shard has opened, shard 0
also records the ids of the databases at every position. Every later
open compares. So a node is refused at startup when it lists the DSNs in
another order, lists more or fewer, lists one database twice, lists a
shard of another list, or lists another database (an empty one, say) at
a position a database has already served; a single-DSN node pointed at
one shard of a list is refused too. A database that already holds
channels but has no identity (it served a single-DSN deployment) cannot
join a list, because the channels that now hash elsewhere would be
stranded. A single-DSN node creates no table and writes nothing for
this; it only reads, at startup, whether the table exists in its schema.
Stop single-DSN nodes before first starting a list on their database: a
node already running checks nothing after it starts.

Each shard's schema also carries its cluster identity row (§11), and
every shard records shard 0's deployment id, so a shard whose row names
another cluster is refused like a shard of another list.

What a failed first start records stays: if one shard cannot be reached,
the shards that opened keep their identity, and a later start must use
the same list. To start again with another list while no shard holds
channels yet, drop `shard_identity` in each shard's schema.

**A shard that is down.** A node that cannot reach one shard cannot serve
that shard's channels, but it can serve the rest, and every node loses the
same shard at the same moment. If one shard down took a node out of
rotation, the load balancer would drain the whole fleet and the healthy
shards' channels would become unreachable too. So `/readyz` (the
`Sharded` `Ping`) pings every shard at once and reports ready while
shard 0 and a majority of the shards answer; a node that reaches no more
than half of them, or not shard 0 (where it checks the list it was given
at startup), leaves rotation. With two shards a majority is both. Each
shard's result is `ably_storage_shard_ready{shard}` (1 or 0, as of the
last readiness check).

The honest consequence: while a shard is down its channels are
unavailable and the others serve. A publish, an attach or a history read
on a down shard's channel fails fast with 50003 (`storage.ErrUnavailable`:
503 over REST, a NACK or channel `ERROR` over the realtime connection),
which an SDK retries; it does not wait on the dead database. The bound
comes from the pool: a connection attempt gives up after 5 s and the
liveness ping of an idle connection on acquire after 2 s, unless the DSN
sets its own `connect_timeout`, so a call fails in about the time it
takes to find every idle connection dead and fail one dial (a stopped
server, which refuses connections, answers at once). A batched publish
(§6.3) fails after its two commit attempts, each bounded the same way.
On the `postgres` bus an attach first waits for its LISTEN on the
shard's LISTEN connection, which is down with the shard; that wait is
bounded at 10 s. The bound also applies behind the queue of re-LISTENs
after a LISTEN reconnect on a node holding very many channels, where an
attach can then fail with 50003 and be retried although nothing is down.
Nothing is lost: what was acknowledged is in the shard, and its
subscribers resume from it when it is back.

**Not supported: resharding, migration, rebalancing.** The count cannot
change without moving data, and there is no tool that moves it. A hot
channel stays on one shard and is still bound by one row lock (§6.3);
sharding raises cluster write throughput to the sum of the shards', not a
single channel's. All nodes must list the same DSNs in the same order.

Series (§10): `ably_storage_shards` (gauge), the number of shards; 1 for
a single DSN; and, with two or more, `ably_storage_shard_ready{shard}`
(gauge, 1 or 0), each shard's result in the last readiness check.

## 7. Pub/Sub

Pub/sub turns a *publish* (originating from any node, via WS or REST)
into entries appended to the **local** Channel's linked list (§5.1)
on every node that has attachments to that channel. The flow is
unified across deployment modes: publish-path callers call
`channel.Publish(ctx, msgs)`, the storage backend persists, and the
Appender callback registered against each ChannelStore delivers the
committed cm to `channel.Append(cm)`. The Appender (`Append`, and
`Discontinuity` for a gap it cannot fill) is the only writer to the
linked list in every mode.

Presence enter/update/leave ride this exact path: a presence operation
is a cm like any other (carrying `Presence` rather than `Messages`) and
reaches subscribers through the identical `storage → Append` mechanism,
including cross-node delivery over the cluster bus (§7.2, §12.2).

### 7.1 Single-process modes (`memory`, `disk`)

The Appender fires synchronously, inside the storage's `Store` call,
right after the persist commits. A publish is:

1. Authorise.
2. `channel.Publish(ctx, messages)` → `store.Store(ctx, messages)` —
   atomically:
   - checks any contained `Message.id` against this channel's
     idempotency index; on hit, returns the originally-persisted
     ChannelMessage with `idempotent=true` (no new row, no Appender
     call — the original was delivered when first persisted);
   - otherwise mints a fresh `channelSerial` (§8), stamps each
     `Message.serial = channelSerial + ":" + idx`, persists, and
     calls `appender.Append(cm)` — which is `core.Channel.Append`,
     linking the cm onto the live list.

Local subscribers parked on the previous tail's wake channels wake up and
observe the new entry. ACK/201 fires once `Publish` returns; the
linked-list update has already happened by then.

### 7.2 Cluster bus

In cluster mode Postgres is always the store: it mints each channel's
serials under the `channels` row lock, holds the log that serves resume
and history, and commits every publish before the ACK. The **bus** is
only how a committed cm reaches the other nodes that hold its channel.
`--bus` selects it; the rest of the server does not change. The bus sits
behind a small seam inside the postgres backend
(`internal/storage/postgres/bus.go`).

| `--bus` | How a committed cm reaches other nodes | Extra infrastructure | What bounds it |
|---|---|---|---|
| `pgnotify` (default) | A `pg_notify` inside the publish transaction on one global channel; every node receives every notification and reads each cm back | none | The notify commit lock (below) and one read-back loop per node |
| `postgres` | Per-channel LISTEN; the cm inline in the payload; one ordered worker per channel. `--postgres-notify-mode=coalesced` (default) sends a wake-up outside the transaction; `transactional` keeps one NOTIFY per write inside it | none | One primary's commit rate, and Postgres's own cost of delivering notifications |
| `nats` | After commit, the cm (or a pointer) is published to the channel's NATS subject; nodes subscribe per bound channel, fanned into a fixed set of dispatch workers | a NATS core server or cluster | One primary's commit rate |

`pgnotify` is the default so the shipped behaviour stays the default
until the deployment sizes are written from measured numbers.

**The publish path, common to every bus.** The publish transaction mints
the serial under the channels-row lock and writes the rows. The bus then
runs a hook inside the transaction (`pgnotify` and transactional
`postgres` NOTIFY here, so listeners see the cm only if it commits), the
transaction commits, and the bus runs a hook after commit (the NATS
publish, the coalesced wake-up mark, the publisher fast path). The ACK is
sent after the commit in every mode. With publish batching (§6.3) the same two hooks
run for every cm of a batch, the first queued into the batch's last round
trip before `COMMIT`.

#### pgnotify

The bus as shipped. The publish transaction emits
`pg_notify('ably_channel', '{"channel":"...","serial":"..."}')`. Every
node's LISTEN goroutine, including the publisher's, receives every
notification, looks up the local `ChannelStore` for that channel, reads
the cm back by `(channel, channel_serial)` and calls `appender.Append(cm)`.
The publisher's own subscribers see the publish through this same round
trip; there is no fast path. Notifications for channels not bound on the
node are dropped; the cm stays in the log for a later `ATTACH` (§4.3).

Postgres delivers notifications at most once. When the LISTEN connection
drops, the goroutine re-dials with capped exponential backoff,
re-`LISTEN`s, and then reconciles each bound channel from
`History(AfterChannelSerial: lastSeen)` (messages, presence and
annotations, merged in serial order) before it resumes. A per-channel high-water mark drops any
cm at or below the last one delivered, so the reconcile and a buffered
notification never deliver a cm twice. The store joins the dispatch map
before the bind reads the channel's watermark, so no notification
committed after the read is missed; one that arrives before
`Initialize` is held and replayed after it, filtered against the
watermark, which also seeds the mark. A reconnect that asks for a
reconcile while a channel is still binding defers it: the bind runs it
from the watermark and merges it with the held notifications before the
channel goes live, so a held notification cannot move the mark past a
cm whose notification was lost in the drop.

Two ceilings limit this bus. Postgres serialises every transaction that
issued a NOTIFY on one lock, shared by every database in the Postgres
cluster and held until the commit record is flushed, so NOTIFYing
publishes commit one at a time and cannot group-commit. And every node
receives the whole cluster's notifications on one goroutine that runs a
SELECT before it takes the next one. Adding nodes raises neither.

#### Chained delivery (postgres and nats)

The `postgres` and `nats` buses share one delivery point
(`internal/storage/postgres/chain.go`):

- **Predecessor.** The publish transaction reads the channel's previous
  serial under the same channels-row lock, in the same round trip as the
  serial advance. Every bus message names that predecessor.
- **Bind order.** `Storage.Channel(name, appender)` puts the bus
  subscription in place (a LISTEN, or a NATS SUB confirmed by a flush)
  before it reads the channel's watermark. A cm committed after the read
  therefore reaches the node; one committed before it sorts at or below
  the watermark, which seeds the delivery point's mark. Messages that
  arrive while the bind is in flight are held until the seed.
- **Publisher fast path.** Straight after commit the publishing node
  offers the cm to its own delivery point for the channel. Local
  subscribers do not wait for the bus; the bus echo is a duplicate the
  mark drops.
- **Order.** A cm whose predecessor is the last delivered serial is
  appended at once. A cm that arrives ahead of its predecessor is held.
  If the predecessor has not arrived within 100 ms, the missing range is
  read from the log (all kinds, serial order, a page at a time) and
  delivered. The mark's lock is held across `Appender.Append`, so a
  Channel sees every cm once and in serial order whichever path
  delivered it. Held cms are bounded: a channel holds at most 1,024 with
  a body (`maxPendingHold`). Normally the hold is bounded by publish rate
  times 100 ms, but a flood of out-of-order bus messages (a hostile or
  buggy sender; the bus is a trusted network) could hold 256 KiB bodies
  without limit. Past the cap a cm keeps its serial and loses its body,
  and a gap fill is forced after about 10 ms; it reads the range, bodies
  included, from the log, so nothing is lost, only read from Postgres
  instead of the bus.
- **Reconcile.** After the bus connection comes back, the channels in the
  sweep scope (below: by default the bound channels with a subscriber on
  this node) are caught up from their mark, 500 channels per query, after
  a random wait of up to a quarter of the sweep interval (7.5 s at the
  default). A node runs at most 4 of these batched catch-up queries (the
  reconcile's and the sweep's) at once, over all its shards. A cluster-wide
  bus blip reaches every node at the same moment; without the scope, the
  wait and the bound, every node would read every channel it holds
  against the same primaries at once. A bound channel with no subscriber
  is not reconciled: as for a bus message lost while it had none, the next
  cm's predecessor reveals the gap, and the sweep catches it up within two
  intervals of it gaining a subscriber. A channel reconcile finds with no
  subscriber also drops its local presence member set, as the sweep does.
- **Sweep.** Every sweep interval (`--bus-sweep-interval`, default 30 s
  for both buses) each node reads the committed serial of the channels
  in its sweep scope, 1,000 per query and one query per shard's
  database, and catches up any channel still behind the serial the
  previous sweep saw. A cm that old whose bus message has not arrived is
  treated as lost, not late.
- **Retention.** Every read of the log from a channel's mark (gap fill,
  catch-up, reconcile, sweep, and on `pgnotify` the reconcile from
  history) is complete only while the log still holds what came after
  the mark. Every cm after the mark was minted after the mark's own cm
  and after the last time the node knew the channel had nothing past
  its mark (the bind's watermark read, or a bus sweep that found the
  channel caught up). If either is at or above the retention floor
  (`RetainedSince`, the resume floor of §4.3, computed locally from the
  measured database clock offset), every cm the read looks for is still
  held and the plain read is used. Otherwise the read carries a check,
  in the same statement as the range on the chaining buses and as two
  point reads around the history read on `pgnotify`: the channel's
  current serial and whether the cm at the mark is still in the log.
  The retention sweep drops a class's leaves oldest first and stops at
  the first that will not detach (§6.3), so while the cm at the mark is
  held, so is every later cm of the channel. This holds per class: a
  namespace moved between classes by a configuration change has rows in
  both, and is not covered until its older rows have aged out. When the
  channel has moved past the mark and the cm at the mark is gone, the
  read cannot prove that nothing aged out between the mark and the
  oldest cm it returned. The node then signals a **discontinuity** to
  the channel before the cms that survived (`storage.Discontinuous`, in
  delivery order under the mark's lock), delivers the survivors, and,
  once an unbounded read has reached the end of the log, moves the mark
  to the channel's serial as of the read, so the expired cms are not
  looked for again. The channel drops its local presence member set,
  which re-seeds from the store on the next `SYNC` (§12.4), and links a
  discontinuity marker into its live list. Each attachment that reaches
  the marker sends its client an `ATTACHED` without `RESUMED`, error
  80016, at its current `channelSerial`, followed for a
  `PRESENCE_SUBSCRIBE` attachment by the re-seeded set (`HAS_PRESENCE`
  and a `SYNC` when it has members). The cms in the gap are not
  replayed; the client is told so that it can reconcile (§4.3). A gap
  the bus revealed (a held cm's predecessor) that the log no longer has
  is signalled the same way, at the point of the skip. The signal can
  be spurious, never missing: a channel last proven caught up longer
  than the retention window ago, whose last cm has aged out and which
  then received a cm while the node was off the bus, is signalled
  although nothing was lost, because the log does not record what
  preceded the new cm. On `pgnotify`, which has no sweep, "last proven"
  is the bind. The proof assumes a publish commits within the one-second
  clock margin of minting its serial (the mint holds the channel's row
  lock until commit); a transaction stalled longer than that between the
  two, and longer than the window, could escape it. A spurious signal costs the client a reconcile, and an
  SDK re-enters its own presence members on an `ATTACHED` without
  `RESUMED` (RTP17i). Counted in
  `ably_channel_discontinuities_total{reason}` (§10).
- **Sweep scope.** The sweep reads only bound channels with a subscriber
  on this node: an open
  attachment, or a presence member the node has seen enter and not leave
  (whose LEAVE eviction waits for). A channel the sweep finds with
  neither also drops its local presence member set (§12.4): that set is
  folded from the delivered cms, so a presence cm lost while nothing was
  attached would stay missing from it, unswept, and be served to the
  next attach. The first sweep that finds the channel so drops the set,
  and the next attach seeds it again from the store (one `Members`
  read). An attach that comes back before that sweep is served the set
  as it is; a cm lost meanwhile is then repaired by the sweep within two
  intervals, as for any subscribed channel, and reaches the attachment
  as a live presence event. An attach with the default modes includes
  `PRESENCE_SUBSCRIBE`, so it seeds a set even on a channel with no
  presence; dropping the set rather than sweeping it keeps such a
  channel out of the sweep once its last attachment closes. A channel
  bound only by a REST request, or kept bound after its last detach until eviction, has
  nobody on this node a lost cm could be late for, so reading it is
  waste: in a 15-minute shape-D run, sweeping every bound channel took
  about a quarter of Postgres's time. The bound: a lost bus message on a subscribed channel
  with no later cm is delivered up to two intervals late (60 s at the
  default). On a channel with no local subscriber nothing is waiting for
  it. The binding goes on receiving the bus, so a later cm's predecessor
  still reveals the gap; if none comes, the lost tail is delivered
  within two intervals of the channel gaining a subscriber, and a channel
  evicted meanwhile re-seeds from the watermark on its next bind, which
  covers the tail. Nothing is lost. (A receiver cannot tell which
  channels it missed a message on, so there is no cheaper "dirty set" to
  sweep instead.) The postgres bus's coalesced overflow (below) is
  delivered by the same sweep, so an overflowed write is also up to two
  intervals late; lower the interval if that matters more than the
  sweep's cost.

#### postgres

Each Ably channel has its own Postgres notification channel: `ably_c_`
plus the hex of the first 16 bytes of SHA-256(schema, NUL, channel name),
a short lower-case identifier whatever the name. The schema
(`current_schema()`) is in the hash because NOTIFY is scoped per database,
not per schema. A node LISTENs on a channel when it binds it and
UNLISTENs when it releases it, so it receives notifications only for the
channels it holds.

In **transactional** mode the transaction NOTIFYs the channel's own
notification channel with `{channel, serial, prev}` and, when the payload
stays under 7,900 bytes, the cm's rows exactly as stored (one msgpack
payload per row plus the annotation summary column), so receivers skip
the read-back. `pg_notify` rejects payloads of 8,000 bytes or more, so a
bigger cm sends the pointer only. This mode removes the per-node ceilings
of `pgnotify` but keeps the notify commit lock.

In **coalesced** mode (the default) writes commit without NOTIFY. A
per-node notifier sends at most one wake-up per channel per window
(`--postgres-notify-window`, default 50 ms), in statements outside any
transaction, each naming the latest serial the node wrote on the channel.
A receiver answers a wake-up with one range read after its mark, or no
read if it is already past that serial. Remote subscribers pay up to one
window of extra latency; the publisher's fast path is unchanged.

**Choosing the window.** The window trades three things. A shorter window
lowers remote delivery latency (up to one window, about half a window on
average). It also raises notify load: each window sends one wake-up per
channel written in it, and each wake-up makes Postgres signal every
listening backend and every receiving node run a range read. So for
channels written more often than once per window, the wake-up rate is
the number of such channels over the window, whatever the publish rate.
In the laptop runs a 5 ms window made Postgres CPU-bound at about 8,000
publishes/s (a storage benchmark reached 2,758/s at 5 ms against 16,078/s
at 50 ms), while at 50 ms Postgres was not the limit. The server accepts
any window, but logs a warning at startup for one under 20 ms
(`postgres.NotifyWindowWarnBelow`): below that the notify load grows
faster than the latency falls, and a deployment that needs lower remote
latency under load needs the `nats` bus, whose delivery does not go
through Postgres at all.

The LISTEN goroutine only receives and dispatches. Each payload goes to
the bound channel's queue; a channel with queued work has one worker,
started on demand and gone when the queue drains, so channels are
consumed in parallel and each stays in order. A worker waits until its
channel's bind has seeded before it handles anything.

**Overflow policy.**

- *Receive side.* A channel's queue holds at most 1,024 notifications.
  One that finds it full is dropped and the channel is marked; its worker
  then discards what is queued and catches the channel up from the log in
  one range read, which delivers everything the dropped notifications
  announced. The LISTEN goroutine never blocks, so a slow channel cannot
  stall another. Each drop counts in `ably_bus_drops_total`.
- *Coalesced send side.* A wake-up is at most the notification channel
  name plus `{"serial":...,"wake":true}`, whatever the Ably channel name,
  so it can never exceed the payload limit. The pending set holds one
  entry per channel written in the window, so a hot channel costs one
  entry however often it is written. It is capped at
  `--postgres-notify-max-pending` channels (default 65,536): a write to a
  channel not already pending when the set is full is not announced, is
  counted in `ably_bus_coalesced_overflow_total`, and is delivered by the
  receivers' next sweeps. A flush sends 1,000 wake-ups per statement; a
  statement that fails (a Postgres error, or a full NOTIFY queue) puts its
  wake-ups back into the pending set for the next window, within the same
  cap. The notifier flushes one window at a time: when a flush takes
  longer than the window, marks arriving meanwhile coalesce into the next
  flush, so falling behind costs latency and never grows the set beyond
  one entry per channel.

The LISTEN connection re-dials with capped exponential backoff,
re-LISTENs every bound channel in batches of 500 statements, and requests
a reconcile. Postgres still wakes every listening backend on each
notifying commit, and each backend filters the notification against its
own LISTEN list, so per-channel LISTEN moves that filtering from the
nodes into Postgres. Coalescing bounds the notification count for hot
channels.

#### nats

After commit the publishing node publishes one message to the channel's
subject: `ably.cm.`, a namespace token, `.`, then the unpadded URL-safe
base64 of the channel name (`h.<sha256 hex>` for a name too long to
encode). The namespace token is the hex of the first 8 bytes of SHA-256
of the cluster's deployment id, a NUL and the schema name. The deployment
id is a random id the first node to open the schema records in its
cluster identity row (§11), so two clusters sharing one NATS cluster are
on different subjects even when their schemas have the same name (the
default `public` on two Postgres servers, say). The body is a msgpack
envelope of channel, serial, predecessor, the cm as stored (with
annotation summary snapshots), the send time and the deployment id. A cm
whose encoding exceeds `--nats-inline-max-bytes` (default 256 KiB) goes
as a pointer, and receivers read it by serial. The publish transaction
emits no NOTIFY.

**The bus is a trusted network.** A receiver delivers the body an
envelope carries without reading the log: that is what makes the bus
fast. So whoever can publish to the NATS subjects can put a message in
front of every subscriber of a channel. Run NATS on a private network,
or authenticated and encrypted: `nats://user:pass@host` and `tls://host`
URLs work, and `--nats-creds` (a credentials file: user JWT and NKey
seed), `--nats-tls-ca`, `--nats-tls-cert` and `--nats-tls-key` (PEM
files) configure the connection (§9). What the receiver does check is
what would otherwise break a channel rather than one message:

- The serial and a non-empty predecessor must be channelSerials in the
  fixed-width form the database mints (§8), and the predecessor must
  sort below the serial. Anything else is dropped and counted in
  `ably_bus_malformed_total{reason="serial"}`; an envelope that does not
  decode, in `{reason="decode"}`.
- A serial minted more than 5 minutes ahead of the receiver's clock,
  corrected by the database clock offset the retention sweep measures,
  is dropped, counted in `{reason="future"}` and logged once per channel
  per minute. Without the check such a serial would sort above every
  serial the channel mints in that time, and the delivery point would
  drop each of them as a duplicate: the channel would be wedged until it
  was rebound. Serials are minted from the database clock, so a genuine
  one is never that far ahead; if one were, the chain or the sweep would
  still deliver it from the log.
- An envelope whose deployment id is not the receiver's cluster's is
  dropped and counted in `ably_bus_unrouted_total{reason="foreign"}`.

**Receive fan-in.** A node holds one NATS subscription per bound channel,
so it receives only its own channels, but the subscriptions do not each
get a goroutine (nats.go's async `Subscribe` would start one per
subscription, so goroutines would grow one for one with bound channels).
Every subscription is a `ChanSubscribe` into one of 16 bounded dispatch
queues (8,192 messages each), chosen by a hash of the channel name, and
one worker per queue decodes each message and hands it to the channel's
delivery point. A channel always uses the same queue, so its messages are
dispatched in the order the connection read them. The goroutine count is
fixed whatever the number of bound channels. A pointer's body is read off
the worker (at most 32 reads in flight), so a slow log read stalls no
other channel on its queue; the chain holds any later cm of that channel
until the pointer's body is in. A pointer that finds 32 reads in flight
goes to the delivery point without a body, and the gap fill reads it
after the hold. A full queue makes NATS drop the message (a slow
consumer, counted once per episode in `ably_bus_drops_total`), which the
chain repairs from the log. `ably_bus_receive_queue_depth` is the number
of messages waiting across the queues. The queues bound messages, not
bytes: at most 16 × 8,192 messages are buffered, each no larger than
`--nats-inline-max-bytes` (a pointer is small), against nats.go's
default of 64 MiB per subscription before the fan-in.

`--nats-url` may list the servers of one NATS cluster, comma-separated.
The client connects to one, learns the others from the cluster, and on a
disconnect moves to another (a silent server is detected by pings within
about 15 s). Every reconnect re-sends the node's subscriptions; a flush
confirms the server has them, and then every bound channel is reconciled.
In a cluster the bind's flush confirms only that the node's own server
has the SUB: a publish through another server in the moment before the
interest reaches it over the route can miss the node. That cm is late,
not lost: the next message's predecessor or the sweep recovers it. A slow
consumer that NATS drops messages for counts in `ably_bus_drops_total`
and is repaired the same way.

**Readiness.** In `nats` mode `/readyz` returns 503 while the node has no
NATS connection, and in `postgres` mode while its LISTEN connection is
down: the node cannot receive cross-node deliveries, so it leaves
rotation until it reconnects and reconciles. `pgnotify` reports ready
while the pool pings, as before.

#### Release and re-bind

`Storage.Release(name)` (the hook idle-channel eviction calls, §5.1)
stops delivery to the channel's appender, removes the bus subscription
(UNLISTEN or NATS unsubscribe; `pgnotify` has none) and forgets the
`ChannelStore`. A later `Storage.Channel(name, appender)` binds afresh:
subscription first, then the watermark read that initialises the new
appender and seeds its mark. Every cm committed after that watermark,
including one committed while the channel was released, reaches the new
appender once and in order; nothing at or below it is delivered. A
`ChannelStore` handed out before the release still stores and reads;
its publishes reach whichever appender is bound at the time. A release
and a re-bind of the same channel queue their UNLISTEN and LISTEN in the
order they happened, so the re-bind's LISTEN stays in effect. The caller
serialises `Channel` and `Release` for one name; if they overlap, the
outcome is as if the release came second.

#### Can a delivery be lost when the bus send is not transactional?

Not while the log still holds it; it can be late. On the `postgres`
coalesced and `nats` buses the message is committed before the bus is
told, so the bus message is a hint that the log has moved, and every
loss below is recovered from the log, provided the receiver reads it
within the retention window (`--message-retention`, default 2 minutes;
`--persisted-retention` for persisted namespaces, §6.3). A receiver off
the bus for longer than that cannot recover what aged out: it says so
(the last row, and **Retention** above) rather than carry on as if the
stream were continuous.

| Failure | pgnotify | postgres, transactional | postgres, coalesced | nats |
|---|---|---|---|---|
| Publisher dies between commit and bus send | cannot happen (NOTIFY commits with the write) | cannot happen | wake-up never sent: next sweeps (at most about two intervals) | message never sent: next cm's predecessor (100 ms hold) or next sweeps |
| Receiver's bus connection drops | reconcile from history on reconnect (all kinds) | reconcile from the log on reconnect | same | same, after a flush confirms the re-sent SUBs |
| A NATS server in the cluster dies | n/a | n/a | n/a | client moves to another server, then reconciles |
| Receiver falls behind | the node's one read-back loop lags; nothing is dropped | full queue: drop, then one catch-up read | same | full dispatch queue, NATS slow-consumer drop: predecessor gap or sweep |
| Read of a pointer or gap fails | the channel is marked; its log is replayed from the mark before any later cm is delivered alone, retried on each notification until it succeeds | retried from the log with backoff | same | same |
| Postgres primary fails over | publishes NACK; nothing acknowledged is lost | same | same | same |
| Receiver off the bus for longer than the retention window | the log cannot prove continuity; the node signals a discontinuity on the channel (`ATTACHED` without `RESUMED`, error 80016) and re-seeds its presence set; cms in the gap are not replayed (§4.3, §12.4) | same | same | same |

Worst case for the chained buses is about two sweep intervals late,
counted from the moment the channel has a subscriber on the receiving
node (the sweep scope above), as long as that is inside the retention
window; past it the cm is reported missing, not delivered. The
load tests check this rather than assume it: the serial-continuity
check fails on any gap or duplicate.

## 8. Identifiers & ordering

- **connectionId**: 12-char base64 of random 9 bytes, generated on `CONNECTED`.
  Process-local; never persisted, never recoverable.
- **connectionKey**: the opaque key carried in `CONNECTED`'s
  `connectionDetails.connectionKey` for an SDK to resume with —
  `connectionId` plus a truncated HMAC-SHA256 suffix over it, keyed with a
  secret random per process (never persisted or shared across a restart or
  cluster). The suffix authenticates the pairing: a bearer of just the bare
  `connectionId` (e.g. one observed on a delivered `Message.connectionId`)
  cannot present it as a resume/recover key for a connection it doesn't
  own. Connection-state resume itself is a non-goal (§1, §11) — no
  attached-channel or message-delivery state is replayed — but a
  resume/recover key that authenticates **does** retain its
  `connectionId` on the new connection, since identity continuity costs
  nothing beyond verifying the key; a key that doesn't authenticate (wrong
  pairing, or malformed) gets a fresh `connectionId` and the `CONNECTED`
  carries error `80018` so the SDK knows the resume/recover failed. It is
  also the key a REST publish sets on a message to **publish on behalf
  of** that live connection (see §3.2): the realtime endpoint keeps an
  in-process registry of live connections indexed by `connectionId`, and
  the REST publish path authenticates the supplied connectionKey the same
  way before resolving it to the target connection, stamping only its
  `connectionId` onto the stored and delivered message — never the target
  connection's `clientId`, which stays whatever the REST request's own
  resolved identity produced. The `connectionKey` field itself is
  inbound-only — it is stripped before the message is persisted or fanned
  out. Resolution is
  **per-node**: the registry is process-local, so in cluster mode a
  connectionKey issued by another node is unknown and the publish is
  rejected with `40006` (invalid connectionKey) rather than routed — an
  accepted limitation under the single-client/locked-down deployment model
  (§1), where publisher and target connection share a node.
- **clientId**: optional, resolved at auth time per §3.2. The server stamps
  it onto every outbound `Message.clientId` published by this connection,
  and rejects inbound frames that try to set a different value.
- **msgSerial** (per-connection publish counter): `*int64` on
  `ProtocolMessage`, assigned by the client; the server echoes it on
  `ACK`/`NACK` so the SDK can address publish acknowledgements. Every
  `ACK`/`NACK` carries an explicit `msgSerial`, including `0` for the
  first publish on a connection — SDKs correlate acknowledgements
  positionally and treat an absent serial as invalid — while frames the
  server never stamps (`HEARTBEAT`, `CONNECTED`, `MESSAGE` deliveries)
  omit the field; the pointer distinguishes "publish serial 0" from
  "no serial". An inbound frame's serial is read as absent-means-0.
  Distinct from `channelSerial` below — this is the wire field for
  publish flow control, not the canonical message ordering identifier.
- **ChannelMessage**: the atomic unit of a publish — one inbound
  REST request, or one `MESSAGE` frame carrying `messages[]`, lands
  on a channel as exactly one ChannelMessage containing one or more
  contained Messages. It is also the unit subscribers observe on the
  wire (one outbound `MESSAGE` frame per ChannelMessage) and the
  unit storage persists. A presence publish is the same unit carrying
  `Presence []*PresenceMessage` instead of `Messages` (§12.1), observed
  on the wire as one `PRESENCE` frame.
- **ChannelMessage.id** (message-publish batch id): the idempotency key
  for a message publish. When the publisher supplies no message ids, the
  server generates a random 8-character base64 batch id; when the
  publisher supplies ids, the batch id is derived from them. Either way
  the server stamps each contained `Message.id = "<batchID>:<idx>"`
  (idx unpadded, so a single-message publish stamps `"<batchID>:0"`),
  and the batch id is what storage indexes for idempotency (below). A
  client that supplies ids on a multi-message publish must make them
  conform to `"<batchID>:<idx>"`; a mismatch is rejected (`NACK` on WS,
  `400` on REST). A single-message publish accepts any client id (the
  batch id is that id with a trailing `:0` trimmed). The REST publish
  response's `messageId` is this stamped first-message id (§2.2).
- **PresenceMessage**: a single presence operation within a presence
  ChannelMessage — the presence-stream analogue of Message. It carries
  an `action` (ENTER/UPDATE/LEAVE inbound; PRESENT in sync; LEAVE/ABSENT
  outbound), a `clientId`, a server-stamped `connectionId`, and optional
  `data`. Like Message it splits `id` (optional, client-supplied
  idempotency key) from `serial` (server-assigned, `<channelSerial>:<idx>`).
  A member's key in the presence set is `connectionId:clientId`.
- **channelSerial** (atomic-publish identifier): a
  lexicographically-sortable string assigned by the server when a
  ChannelMessage lands on a channel, modelled on Ably cloud's
  internal format:

  ```
  <timestamp>-<counter>@<seriesId>
  ```

  - `timestamp` — current wall-clock time in milliseconds, zero-padded
    to 14 digits (~3000 years of headroom).
  - `counter` — zero-padded 3-digit per-`(timestamp, seriesId)` counter,
    incremented when multiple serials are minted within the same
    millisecond. Resets to `000` when the timestamp advances.
  - `seriesId` — a fixed-length random string generated at process
    start; disambiguates serials minted in the same millisecond on
    different nodes in cluster mode.

  channelSerials are the discrete attach/resume points in a channel's
  stream. On `ATTACHED` the wire field carries the confirmed attach
  point — the channelSerial from which the client's subscription
  begins. On each subsequent outbound `MESSAGE` frame, it carries the
  channelSerial of the ChannelMessage being delivered. The client
  retains the most recent channelSerial it has seen and sends it back
  on a future `ATTACH` to request continuation from that point (see
  §4.3).

  Lexicographic comparison of channelSerials matches publish order,
  which lets the disk backend (bbolt) and cluster backend (Postgres)
  use channelSerial directly as the primary key without a separate
  ordering column.

  In cluster mode the order is decided once, at commit, by the channel's
  row in `channels`: the serial is minted under that row lock, so it is
  a total order per channel across nodes. With publish batching (§6.3)
  a node's publishes to one channel go through one lane, in arrival
  order, and a channel is in at most one in-flight batch, so they are
  minted in that order: per-publisher order within a channel is kept.
  Publishes of different nodes to one channel are ordered by which batch
  takes the row lock first; a batch that finds the row locked defers the
  channel rather than waiting (§6.3).
- **Message.serial**: the server-assigned **identity** of an individual
  message, of the form

  ```
  <channelSerial>:<idx>
  ```

  where `idx` is a zero-padded 3-digit position within the
  ChannelMessage (`000` for a single-message publish). All messages in a
  batch share the channelSerial prefix and differ only by `idx`. For a
  plain publish (`action: create`) the serial is the position of that
  publish. Crucially, it is **stable across versions**: when the message
  is later updated, deleted, or appended (§13), every version carries the
  *same* `serial` as the original create — the serial names the message,
  not the version.
- **Message.version**: present once mutable messages are in play (§13.1),
  the server-assigned identity of a *single version* of a message. It
  has the same `<channelSerial>:<idx>` shape — the position of the
  publish that produced this version — so a create has `version == serial`
  and each subsequent update/delete/append gets a fresh, strictly-greater
  `version`. Lexicographic comparison of versions gives newest-wins
  ordering. The wire `version` object also carries operation metadata
  (timestamp, the operating `clientId`, an optional description, and
  optional metadata).
- **Message.id**: the per-message identifier used for idempotent
  publishing, always of the form `"<batchID>:<idx>"` where `batchID` is
  the containing ChannelMessage's batch id (above). The server stamps it
  on every publish — deriving `batchID` from client-supplied ids, or
  generating one when none is supplied. The server enforces uniqueness
  per channel within the message retention window: because every message
  in a batch shares the same `batchID`, a repeat of a client-idempotent
  publish collides on its first message id and returns the original
  publish's `channelSerial` without re-appending. A publish whose batch
  id was server-generated carries a fresh random `batchID` each time, so
  it is always treated as new. `batchID` is opaque to the server —
  clients typically use a UUID or a deterministic hash of payload +
  intent.
- **Message.extras**: an optional free-form JSON object the client
  attaches to a message — headers, push metadata, and the AI Transport
  SDK's `extras.ai`. The server treats it as opaque and preserves it
  verbatim through publish, fan-out, storage, history and REST reads; a
  mutation carries the target's extras forward unless it supplies its own,
  which replaces the whole object (shallow-mixin, §13.2). `PresenceMessage`
  and `Annotation` carry the same `extras` field with identical semantics.
- **Message.timestamp**: the message's **create time** (server wall-clock ms,
  derived from the create serial), stamped at create and carried forward
  **unchanged** onto every later version — update, delete, append aggregate,
  and append delta alike. It is the message-identity timestamp, distinct from
  `version.timestamp`, which carries the *operation* time of the version that
  produced it. This mirrors the reference's `buildUpdateMessage`, whose
  top-level `timestamp` is the original message's timestamp. The field is
  `omitempty`: an unstamped (zero) value is dropped on the wire, so every
  delivery must carry it (an SDK reads the top-level timestamp as the create
  time and drives its retention/reorder clock from it).

Replay on `ATTACH` is a bounded history read from storage between the
client-supplied `channelSerial` and the channel's current head, streamed
after the `ATTACHED` ack. If the requested serial is older than retained
history the server attaches at the live head, clears
`ATTACHED.flags.RESUMED`, and populates `ATTACHED.error` so the SDK can
surface the discontinuity. A node that cannot prove continuity on the
live stream sends the same `ATTACHED` mid-stream (§4.3, §7.2).

## 9. Configuration

CLI flags (each with an `ABLY_SERVER_*` env var equivalent, named by
upper-casing and underscoring the flag — e.g. `--log-format` is
`ABLY_SERVER_LOG_FORMAT`):

```
--mode {memory|disk|cluster}  default: memory
--listen :8080                HTTP/WS bind
--keys                        appId.keyId:keySecret (repeatable; ABLY_SERVER_KEYS is comma-separated)
--data-dir ./data             disk mode only
--postgres-dsn  postgres://…  cluster mode only; a comma-separated list of URL DSNs shards channels (§6.4)
--bus {pgnotify|postgres|nats}  cluster mode cross-node bus (§7.2); default: pgnotify
--nats-url nats://…           --bus=nats only; a comma-separated list of one NATS cluster's servers
--nats-inline-max-bytes 262144  largest cm the NATS bus carries inline; larger ones go as pointers
--nats-creds                  --bus=nats: NATS credentials file (user JWT and NKey seed); nats://user:pass@host URLs also work (§7.2)
--nats-tls-ca                 --bus=nats: PEM CA the NATS server's certificate chains to; tls:// URLs also work
--nats-tls-cert, --nats-tls-key  --bus=nats: PEM client certificate and key, for a server that verifies clients
--postgres-notify-mode {coalesced|transactional}  --bus=postgres only; default: coalesced
--postgres-notify-window 50ms   coalescing window (coalesced mode)
--postgres-notify-max-pending 65536  cap on channels pending a coalesced wake-up per node
--bus-sweep-interval 0s       chaining buses' safety-net sweep of channels with a local subscriber; 0 = bus default (30s) (§7.2)
--shutdown-grace 10s          window to disconnect existing connections on SIGTERM
--log-level info              one of: trace, debug, info, warn, error
--log-format {text|json}
--debug-listen                pprof on a separate port; disabled if unset
--config ably-server.toml     optional TOML file, see below
--addr-file                   path to write the bound listener address to once listening
--enable-stats-stub           register the GET/POST /stats compatibility stub (§1); default: false (404)
--channel-idle-timeout 60s    evict a channel idle this long and release its storage binding (§5.1); 0 disables
--conn-outbound-max-bytes 1048576  bytes of encoded frames queued per connection before pushes wait (§5.2)
--conn-write-timeout 10s      deadline per frame write, and the longest wait for queue room, before a slow-consumer disconnect (§5.2)
--ws-read-buffer-size 1024    per-connection WebSocket read buffer, bytes (§5.2)
--ws-write-buffer-size 4096   pooled WebSocket write buffer, bytes (§5.2)
--http-idle-timeout 120s      how long an idle HTTP keep-alive connection is kept open (§2.2)
--attachment-seen-max 4096    message serials one attachment remembers for append deltas; oldest evicted (§13.3)
--message-retention 2m        cluster mode: continuity window, the log retention of non-persisted channels (§6.3)
--persisted-retention 24h     cluster mode: log retention of channels in a persisted namespace (§6.3)
--publish-lanes 4             cluster mode: publish batching lanes; 0 = one transaction per publish (§6.3)
--publish-batch-max 200       cluster mode: most publishes in one batch transaction
--publish-linger-max 5ms      cluster mode: in-flight time after which other channels start a second batch
--publish-queue-max 10000     cluster mode: queued publishes per lane before 42910
--presence-max-inflight 0     cluster mode: presence writes committed outside the lanes at once per database; 0 = 4 x --publish-lanes, negative = no bound (§12.5)
```

`--addr-file` writes the listener's resolved `host:port` to the given path
once the bind succeeds, then keeps running. It exists so a parent process
that started the server on `--listen 127.0.0.1:0` can discover the
ephemeral port the OS assigned — the sandbox provisioner (§15) relies on
it. The write is atomic (a sibling temp file renamed into place), so a
reader polling the path never sees a partial address.

Configuration may also be supplied via an optional TOML config file
(`--config ably-server.toml`), covering the same keys as the flags above
(`mode`, `listen`, `data-dir`, `postgres-dsn`, `bus`, `nats-url`,
`nats-inline-max-bytes`, `nats-creds`, `nats-tls-ca`, `nats-tls-cert`,
`nats-tls-key`, `postgres-notify-mode`, `postgres-notify-window`,
`postgres-notify-max-pending`, `bus-sweep-interval`, `shutdown-grace`,
`log-level`, `log-format`, `debug-listen`, `enable-stats-stub`,
`channel-idle-timeout`, `conn-outbound-max-bytes`, `conn-write-timeout`,
`ws-read-buffer-size`, `ws-write-buffer-size`, `http-idle-timeout`,
`attachment-seen-max`, `message-retention`, `persisted-retention`, `publish-lanes`,
`publish-batch-max`, `publish-linger-max`,
`publish-queue-max`,
`presence-max-inflight` —
`shutdown-grace`, `postgres-notify-window`, `bus-sweep-interval`,
`channel-idle-timeout`, `conn-write-timeout`, `http-idle-timeout`, the
retentions and `publish-linger-max` as duration strings, e.g. `"10s"`,
the sizes and counts as integers). API keys are
declared as structured
`[[keys]]` entries, each a `key` spec plus an optional `capability` — an
`x-ably-capability`-format JSON object string (§3.1) that scopes what the
key grants; omitting it grants the full capability, like a `--keys` flag
or `ABLY_SERVER_KEYS` entry. `[[keys]]` is the only file-tier source of
API keys, and the only one that can carry a narrowing capability. A
malformed capability string is a startup error.

```toml
[[keys]]
key = "app.subscriber:s3cr3t"
capability = '{"chat:*":["subscribe"]}'
```

Every key is optional. Resolution order,
highest priority first: flag > env > config file > hardcoded default —
so a flag always wins, an env var beats the file, and the file only
supplies a value nothing more specific set.

A bus setting the chosen `--bus` does not use (a `--nats-*` setting under
`pgnotify` or `postgres`, a `--postgres-notify-*` setting under `pgnotify`
or `nats`, `--bus-sweep-interval` under `pgnotify`), given by flag, env or file,
is named in a warning at startup rather than dropped silently. The bus,
the retentions and the persisted namespaces must be the same on every
node of a cluster, which the database enforces at startup (§11).

**Removed settings.** The settings below existed to switch a fix off for
an A/B comparison in the scale proof; each was removed once the fix was
measured, and the behaviour it selected by default is now the only one.
A flag from this list is now a startup error; its env var is ignored; a
TOML key from it, like any key the file format does not define, is named
in a warning at startup.

- `--publish-bind-on-write` (`publish-bind-on-write`): replaced by the
  write-only publish path (§5.1) and in-batch channel row creation (§6.3,
  "Channel rows"), which were its `false` default.
- `--bus-sweep-scope` (`bus-sweep-scope`): the sweep always reads only
  the bound channels with a local subscriber, its `subscribed` default
  (§7.2 "Sweep scope"); `bound` is gone.
- `--publish-linger-min` (`publish-linger-min`) and its gauge
  `ably_publish_linger_min_seconds`: an idle lane always commits its first
  publish at once, the leading edge that was its `0s` default (§6.3
  "Publish batching"). A linger floor gave no measured gain in the scale
  runs; the batch depth it aimed at is set by the lane count instead.
- `--presence-sync-source` (`presence-sync-source`): a node always serves
  an attach's presence `SYNC` from its own member set, the `local`
  default (§12.4), and reads the store only when seeding that set fails
  (`ably_presence_syncs_total{snapshot="fallback"}`); `store`, a store
  read per attach, is gone with its `snapshot="store"` series value.
- `--presence-batching` (`presence-batching`): presence writes always join
  the publish lanes' batches when there are lanes, its `true` default
  (§6.3 "Presence in a batch"). Turning batching off for everything,
  `--publish-lanes=0`, is the one way to commit presence on its own, and
  `--presence-max-inflight` stays to bound the writes committed outside
  the lanes.
- `--presence-lease-mode` (`presence-lease-mode`): presence liveness is
  always one lease per node, the `node` default (§12.5); `member`, a
  lease on every member row, is gone. A rolling upgrade from a node
  that ran `member` is safe (§12.5 "Upgrading from member lease mode").

The config file additionally carries the startup fixtures — everything
the server boots with is visible in one file, structured like the Ably
*test-app-setup* `post_apps` shape (the sandbox provisioner translates
that JSON into this config rather than the server parsing it):

- `[[namespaces]]` — a namespace `id` plus the `persisted`,
  `mutableMessages`, and `pushEnabled` feature flags. `persisted` selects
  the retention class of the namespace's channels in cluster mode (§6.3).
  `mutableMessages` does not gate update, delete or append (they work on
  every channel); it only selects which delivered messages an attachment
  remembers for append deltas (§13.3). `pushEnabled` is **parsed and
  recorded but behaviourally inert**; it exists so a provisioner can
  round-trip the full app shape. A namespace with no `id` is a startup
  error.
- `[[channels]]` — a channel `name` plus nested `[[channels.presence]]`
  member entries (`clientId`, `data`, `encoding`). At startup, before the
  listener opens, each member is entered through the normal
  `StorePresence` path so it lands in both the membership set and
  presence history, with a server-synthesized `connectionId`;
  `clientId`/`data`/`encoding` round-trip verbatim (encoding is opaque —
  cipher payloads are never decoded). Seeded members are static (§12.5).
  A channel with no `name` or a member with no `clientId` is a startup
  error. This exists purely so the ably-go presence suite, which the
  cloud sandbox provisions these members for, can run against a local
  server.

```toml
[[namespaces]]
id = "persisted"
persisted = true

[[channels]]
name = "persisted:presence_fixtures"

  [[channels.presence]]
  clientId = "client_string"
  data = "This is a string clientData payload"
```

## 10. Observability

- **Logs**: structured (`slog`), `text` for dev, `json` for prod. Level
  policy: `info` is steady-state — process lifecycle (startup/config,
  listener bound, shutdown) and connection open/close, so a quiet default
  like a database's. `debug` adds state changes (channel attach/detach);
  `trace` (a custom level below `slog`'s `debug`) adds per-operation
  detail (every frame received). Every line is self-contained, and
  connection-scoped lines carry `connId`.
- **Metrics**: Prometheus at `/metrics`, served on the `--debug-listen`
  listener alongside pprof — not the main listener, so a publicly reachable
  server doesn't leak operational detail to anyone who can reach it.
  Disabled by default since `--debug-listen` is unset; `/healthz`/`/readyz`
  stay on the main listener. The series are process-wide and
  low-cardinality — no per-channel, per-connection, or per-clientId labels:
  - `ably_connections_opened_total` (counter) — WebSocket upgrades.
  - `ably_connections_open` (gauge) — currently-open WebSocket connections.
  - `ably_connection_lifetime_seconds` (histogram) — connection lifetime,
    upgrade to teardown.
  - `ably_attachments_total` (counter) — channel attachments established.
  - `ably_messages_published_total` (counter) — inbound publishes accepted
    (WebSocket + REST).
  - `ably_messages_delivered_total` (counter) — outbound `MESSAGE` frames
    forwarded to attachments.
  - `ably_publish_latency_seconds` (histogram) — inbound publish to
    storage-commit/ACK.
  - `ably_http_requests_total{route,method,status}` (counter) — REST requests
    by matched route pattern, method, and response status.
  - `ably_channels_bound` (gauge) — channels currently bound on this node
    (§5.1).
  - `ably_channel_binds_total` (counter) — channel binds: first use, or a
    rebind after eviction.
  - `ably_channel_evictions_total` (counter) — idle channels evicted.
  - `ably_channel_release_errors_total` (counter) — storage `Release` calls
    that failed during eviction.
  - `ably_channel_unbound_publishes_total` (counter) — REST publishes that
    took the write-only path: stored on a channel not bound on this node,
    without binding it (§5.1).
  - `ably_channel_discontinuities_total{reason}` (counter) — discontinuities
    signalled on a bound channel, each sent to its attachments as an
    `ATTACHED` without `RESUMED`, error 80016 (§7.2): `retention` (a read
    of the log from the delivery mark started below the retention floor
    and could not prove nothing had aged out) or `log_gap` (a gap the bus
    revealed was not in the log and was skipped). Any increase means some
    clients were told that cms may be missing.
  - `ably_slow_consumer_disconnects_total{reason}` (counter) — connections
    disconnected for not reading fast enough: `queue_full` (the outbound
    queue stayed at its bound for the write timeout) or `write_timeout` (a
    socket write missed its deadline) (§5.2).
  - Cluster mode only, from the retention sweep (§6.3):
    `ably_storage_partitions_created_total{table}`,
    `ably_storage_partitions_dropped_total{table}`,
    `ably_storage_retention_errors_total`,
    `ably_storage_partition_drop_lock_timeouts_total` (counters) and
    `ably_storage_log_bytes{table}`, `ably_storage_log_partitions{table}`
    (gauges). `table` is `channel_messages` or `messages`.
  - Cluster mode only: `ably_storage_shards` (gauge), the number of Postgres
    shards (§6.4); 1 for a single DSN. With two or more,
    `ably_storage_shard_ready{shard}` (gauge), 1 while the shard (and its
    bus) answered the last readiness check, else 0. With two or more shards every
    `ably_storage_*` and `ably_publish_*` series above carries a `shard`
    label (the shard's index in the `--postgres-dsn` list), and the
    `ably_bus_*` series below are summed over shards.
  - Cluster mode only, from publish batching (§6.3):
    `ably_publish_lanes`, `ably_publish_linger_max_seconds` (gauges, the
    configuration),
    `ably_publish_batch_size`, `ably_publish_commit_seconds` (histograms),
    `ably_publish_commits_total`, `ably_publish_deferred_total`,
    `ably_publish_batch_retries_total`, `ably_publish_nacks_total{reason}`,
    `ably_publish_server_presence_unbatched_total`,
    `ably_publish_server_presence_forced_total` (counters) and
    `ably_publish_lane_queue_depth{lane}` (gauge).
  - Cluster mode only, from presence liveness (§12.5):
    `ably_presence_reaps_deferred_total` (reaper rounds the reaper guard
    skipped) and `ably_presence_lease_lapses_total` (renewals that found
    the node's own lease had lapsed) (counters).
  - Delivery stages after the append (§5.1, §5.2), from one connection in
    8 so a fan-out to tens of thousands of attachments does not make as
    many observations on one histogram: `ably_delivery_fanout_seconds`
    (histogram, buckets 100 µs to 2.5 s), the time from a cm's append to
    the channel's live list to its frame being queued on the attachment's
    connection, for live cms (not replays); and
    `ably_conn_write_wait_seconds` (histogram, same buckets), the time
    from a frame being queued on the connection's outbound queue to its
    socket write completing. Together they split the node's part of a
    delivery after the append: waking and running the attachment
    goroutine (and encoding, for the first attachment on a shared frame),
    then the connection's write loop. `ably_delivery_fanout_size`
    (gauge) is the largest number of attachments open on a channel when a
    cm was appended to it (the attachments that append fans out to), since
    the previous scrape; reading it resets it, so it is meant for one
    scraper.
  - Presence liveness (§12.5): `ably_presence_grace_leave_errors_total{stage}`
    (counter), grace-window LEAVEs that could not be written, by stage
    (`get_channel`, `publish`), and `ably_presence_reentries_total`
    (counter), members re-entered after the node's presence lease lapsed.
  - Presence sync (§12.4): `ably_presence_syncs_total{snapshot}` (counter),
    the SYNC snapshots served, by how each was obtained: `cached` (the
    channel's current snapshot), `waited` (rebuilt by another attach
    after waiting out the refresh window), `built` (rebuilt from
    the node's member set), `fallback` (a store read because seeding the member set failed); and
    `ably_presence_sync_seeds_total` (counter), the member sets seeded from
    the store: at most one per channel bind, plus one after a skipped bus
    gap (§7.2) and one after each sweep that found the channel with no
    subscribers (§12.4).

  In cluster mode the bus (§7.2) adds `ably_bus_*` series, also process-wide:
  - `ably_bus_info{bus,mode}` (gauge, always 1) — the bus and the postgres
    bus notify mode; `ably_bus_connected`, `ably_bus_bound_channels` and
    `ably_bus_receive_queue_depth` (gauges; the last is the bus messages
    waiting in the nats bus's dispatch queues, 0 on the other buses).
  - Traffic: `ably_bus_published_total`, `ably_bus_publish_errors_total`,
    `ably_bus_pointers_total`, `ably_bus_received_total`,
    `ably_bus_unrouted_total{reason}` (`unbound`: no bound store for the
    channel on this node; `foreign`: a nats bus message of another
    cluster) and `ably_bus_malformed_total{reason}` (`decode`, `serial`,
    `future`; §7.2).
  - Delivery paths, one count per cm appended:
    `ably_bus_inline_deliveries_total`, `ably_bus_fetched_deliveries_total`,
    `ably_bus_fast_path_deliveries_total`,
    `ably_bus_filled_deliveries_total` (log range reads).
  - Recovery: `ably_bus_duplicates_total`, `ably_bus_holds_total`,
    `ably_bus_gap_fills_total`, `ably_bus_fetch_errors_total`,
    `ably_bus_drops_total`, `ably_bus_reconciles_total`,
    `ably_bus_reconciled_channels_total`, `ably_bus_reconcile_seconds_total`,
    `ably_bus_sweeps_total`, `ably_bus_sweep_channels_total` (channels
    whose watermark a sweep read, summed over sweeps: the sweep scope's
    size), `ably_bus_sweep_catch_ups_total`,
    `ably_bus_sweep_seconds_total`.
  - Receive-side stages (histograms, buckets 100 µs to 2.5 s), which
    split the delivery lag below: `ably_bus_receive_queue_wait_seconds`
    (nats bus: from the publishing node's send to a dispatch worker taking
    the message off its shard queue, so NATS transit plus the wait in the
    queue, on the two nodes' clocks; one observation per message
    received), `ably_bus_hold_seconds` (a cm that arrived ahead of its
    predecessor, from the hold to its append, §7.2) and
    `ably_bus_append_seconds` (the time inside the channel's Append for a
    cm the bus delivered, on any bus, including the wake-up of every
    attachment parked on the channel, §5.1). A dispatch worker delivers
    one channel at a time, so a long Append on a channel with many
    attachments shows here and as queue wait for the other channels on
    its shard.
  - `ably_bus_delivery_lag_seconds{path}` (histogram, buckets 1 ms to
    30 s): for each cm a node appends that came from another node, the
    time from its commit to the append, by delivery path (`inline`,
    `fetched`, `filled`, the same paths as the delivery counters). The
    start is the bus message's send time on the nats bus (the envelope
    carries it, taken straight after the commit); on the other buses, and
    for any cm read from the log, it is the cm's stored timestamp (a
    message's version timestamp, which the serial minted under the row
    lock in the publish transaction fixes, so it is within one commit of
    the commit itself; millisecond resolution). The publisher fast path
    is not recorded. The two nodes' clocks are compared, so the
    histogram is only as good as their sync (chrony in the cloud runs).
    It separates time on the bus (and in the node's receive path) from
    time the server spends before the commit and after the append: a
    delivery latency seen by a client, minus this, is the publish path
    plus the connection's own queue.
  - Postgres bus: `ably_bus_listens_total`, `ably_bus_unlistens_total`, and
    in coalesced mode `ably_bus_coalesced_wakeups_sent_total`,
    `ably_bus_coalesced_wakeups_received_total`,
    `ably_bus_coalesced_flushes_total`,
    `ably_bus_coalesced_flush_errors_total`,
    `ably_bus_coalesced_flush_seconds_total` and
    `ably_bus_coalesced_overflow_total`.

  Standard Go runtime and process collectors are also registered.
- **Tracing**: OpenTelemetry, off by default and configured entirely through
  the standard `OTEL_*` environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
  `OTEL_SERVICE_NAME`, `OTEL_TRACES_EXPORTER`, …). With no `OTEL_*` present no
  exporter is built and no goroutine is started — the no-op tracer carries no
  export overhead. When enabled, spans are exported over OTLP/HTTP and cover
  the WebSocket connection lifecycle (`ws.connection`), the publish path
  (`publish` / `channel.publish`), and — via otelhttp — REST request handling.
- **pprof**: behind `--debug-listen` on a separate port, alongside `/metrics`.

## 11. Lifecycle & operations

**Startup.** Each backend bootstraps its storage at `Open` time. The
bbolt backend creates the two top-level buckets if missing (§6.2).
The Postgres backend runs the auto-migrate sweep described in §6.3 —
a session-scoped advisory lock serialises N concurrently-starting
nodes so only one applies migrations, the rest observe the
`schema_migrations` tracker and skip.

**Cluster identity.** Some settings must be the same on every node of a
cluster, and nodes that differ do not fail on their own. A `pgnotify`
node and a `nats` node never deliver to each other; the retention sweep
of whichever node runs it applies that node's retentions and persisted
namespaces to everybody's log (§6.3). So migration 0005 adds a one-row
`cluster_identity` table to the schema, and `Open` checks it under the
shard identity advisory lock (§6.4), so two nodes starting at once on an
empty schema never both write it. The first node to open the schema
records:

| Column | What | A later node that differs |
|---|---|---|
| `deployment_id` | a random id, minted once; the nats bus mixes it into every subject and carries it in every envelope (§7.2) | n/a (a shard of a list must carry shard 0's) |
| `bus` | `--bus` | refused |
| `message_retention`, `persisted_retention` | `--message-retention`, `--persisted-retention` | refused |
| `persisted_namespaces` | the `[[namespaces]]` ids with `persisted = true`, sorted | refused (compared as a set) |
| `min_server_version` | the lowest cluster version that may join (1 today) | a server of a lower version is refused |
| `created_at` | when the row was written | |

A refusal names every difference and the `UPDATE` that records this
node's settings instead. Each shard of a list has its own row, with shard
0's deployment id. The row is written before the node connects its bus,
so a first node that then fails to start (a wrong NATS password, say)
has still recorded its settings; a later start must match them or change
them as below.

*Changing a recorded setting* is a deliberate step, not a flag: stop
every node; run the `UPDATE` the refusal printed, for example
`UPDATE cluster_identity SET bus = 'nats'`, in the schema (in every
shard's schema, for a DSN list); then start the nodes with the new
setting. The deployment id stays. A flag that overwrote the record at
startup was rejected: left in a config file or an environment, it would
overwrite the record on every start, and the check would protect
nothing. Deleting the row instead makes the next node record its own
settings and a new deployment id.

*Upgrading to this version.* A database without the row records the
settings of the first upgraded node; nodes of earlier versions do not
check. On the nats bus the deployment id changes the subjects, so during
a rolling upgrade upgraded and older nodes do not hear each other's bus
messages; their cms still reach each other, late, by the next cm's
predecessor or the sweep (§7.2). Restart the nodes of a nats cluster
together to avoid the window.

**A shard that is down** (§6.4): the node stays in rotation while shard 0
and a majority of the shards answer, and the down shard's channels fail
fast with 50003 until it is back. `ably_storage_shard_ready{shard}` says
which shard. Nothing needs doing on the nodes when it returns: the pool
reconnects on the next call, and its channels' subscribers resume from
the log.

On SIGTERM the server enters a graceful shutdown:

1. Stop accepting new WebSocket and HTTP connections.
2. Walk the registry of live WebSocket connections and, for each, send a
   `DISCONNECTED` frame (so SDKs reconnect elsewhere) and then close the
   socket — which drives the connection's normal teardown, including the
   synthesised presence `LEAVE`s (§12.5). These closures are **paced
   evenly across the `--shutdown-grace` window** (default `10s`) rather
   than fired all at once, so reconnects arrive at the next node
   staggered rather than as a thundering herd. Any connection still open
   at the deadline is force-closed immediately.
3. Concurrently, drain in-flight REST handlers.
4. Wait, bounded by the same grace window, for any delayed presence `LEAVE`
   (§12.5) already past its timer to finish writing, so none fires after
   the storage is closed. Pending ones that have not reached their timer
   are abandoned: the node's members go with it.
5. Close storage. For the Postgres backend this finishes in-flight publish
   batches (up to 5 seconds, then cancels them), cancels the background
   loops, stops every channel's pending gap-fill timer, and cancels a gap
   fill or pointer read in flight (they run on the storage's own context,
   with a 10 second fetch timeout) before it releases the pool, so closing
   never waits on a log read.

**Readiness.** `/readyz` (§2.2) is what an orchestrator or load balancer
should route on. In `memory` and `disk` mode it is always 200. In
`cluster` mode it returns 503, and logs the reason at Warn, unless all of
these hold, each within the probe's 2 s:

- the Postgres pool answers a ping;
- the bus is connected: the LISTEN connection for `--bus=postgres`, the
  NATS connection for `--bus=nats` (§7.2); `pgnotify` does not gate
  readiness on its LISTEN connection, which re-dials and reconciles on
  its own;
- every publish lane is completing its commits: no lane's oldest queued
  publish has waited longer than one commit attempt (15 s), and no batch
  has been committing for longer than two (a commit and its retry). A
  node whose lanes are wedged (a stuck connection, starved goroutines)
  still pings, but cannot ACK a publish; this takes it out of rotation.
  The error names the lane.

With several Postgres shards (§6.4) each condition must hold on every
shard, and the error names the shard. Readiness is independent of the
presence lease (§12.5): the lease bump runs on its own timer whatever the
traffic, so an idle node never lapses, and a lapsed lease is repaired by
re-entry rather than by leaving rotation.

In `cluster` mode each node is fungible. Rolling restart works because
clients are told to reconnect; the next node accepts the new connection
and, on each `ATTACH`, replays missed messages from Postgres using the
client-supplied `channelSerial`. No connection state crosses nodes.

**Upgrading across `0002_partitioned_log`** (once, for a database created
before it; a fresh database, or one already past it, takes ordinary
rolling restarts). This is an offline step, not a rolling one (§6.3):

1. Take a snapshot or backup of the database.
2. Optional, to shorten step 5: delete log rows older than the
   continuity window, in batches, while the old version still runs
   (`DELETE FROM channel_messages WHERE ctid IN (SELECT ctid FROM
   channel_messages WHERE channel_serial < '<14-digit ms time two minutes
   ago>' LIMIT 10000)`, repeated until it deletes nothing; the same on
   `messages` by `message_serial`; then `VACUUM`). Every
   pre-upgrade row lands in the live class and is dropped within a few
   minutes of the upgrade, so older rows are only migration cost. This
   also drops pre-upgrade persisted history, which the upgrade keeps for
   the continuity window only (§6.3).
3. Estimate the downtime. Rows per second is unknown for your hardware.
   Restore the snapshot into a scratch database and run the new binary
   against it once, timing `Open`; that is the downtime to plan for.
   Failing that, `SELECT count(*), pg_size_pretty(pg_total_relation_size(
   'channel_messages')) FROM channel_messages` and
   `pg_total_relation_size('messages')` give the work the attach does:
   it reads each row once and builds one index.
4. Stop every node of the old version (or take them out of the load
   balancer and wait for their connections to drain, then stop them). Do
   not leave any running: the migration cannot take its lock while one
   holds a transaction on the tables, and gives up after about 26
   seconds (§6.3).
5. Start one node of the new version and wait for it to report ready
   (`/readyz` is 200). `Open` does not return, so `/readyz` does not
   answer, until the migration commits. Confirm
   `SELECT version FROM schema_migrations` lists
   `0002_partitioned_log`. If `Open` failed, nothing was applied (the
   transaction rolled back): fix the cause and start again, or start the
   old version.
6. Start the rest of the fleet. Their `Open` finds every migration
   applied and takes no table lock. Starting them before step 5 ends is
   safe (they wait on the migration advisory lock) but their readiness
   checks fail until it ends.

Do not run old and new versions together across this migration: an old
node would write persisted-namespace rows into the live class, and a
resume across the resulting hole would be accepted as continuous.

## 12. Presence

Presence lets clients announce themselves as **members** of a channel —
each identified by a `clientId` — and observe other members entering,
updating their state, and leaving. It is modelled as a second kind of
message riding the channel's existing stream, plus a derived
**membership set** the server materialises so a late-arriving subscriber
can be brought up to date without replaying the whole stream.

### 12.1 Model

A presence operation is a publish like any other: it lands on the
channel as one ChannelMessage and flows through the unified
`storage → Appender.Append` path (§7). The only difference is the
payload — a ChannelMessage carries exactly one of `Messages` (a data
publish), `Presence` (a presence publish), or `Annotations` (an
annotation publish, §14):

```go
type PresenceMessage struct {
    ID           string         // "<connectionId>:<msgSerial>:<index>" or client-supplied (§8, §12.1)
    Serial       string         // server-assigned, "<channelSerial>:<idx>" (§8)
    Action       PresenceAction // ENTER | LEAVE | UPDATE | PRESENT | ABSENT
    ClientID     string
    ConnectionID string
    Data         any
    Encoding     string
    Timestamp    int64
}
```

`Serial` carries the same server-assigned `<channelSerial>:<idx>` split as on
`Message` (§8). `ID` is the presence-newness key SDKs use to order two
operations for the same member (ably-js `newerThan` / RTP2b): a **genuine**
op — one published by a live connection — is stamped
`<connectionId>:<msgSerial>:<index>` at realtime publish (the member's
`connectionId`, the inbound `PRESENCE` frame's `msgSerial`, and the op's
position in that frame's `presence[]`), so the SDK orders it by
`(msgSerial, index)` on the id path. A client may supply its own `ID`, in
which case it is honoured verbatim (idempotency intent, §8). A
**synthesized** event — a fixture-seeded member (§9) or a server-fabricated
teardown/detach LEAVE (§12.5) — has no real connection/msgSerial and stays
**id-less**; the SDK recognises it as synthesized (its id is not prefixed by
its own `connectionId`) and orders it by `Timestamp` with an arrival-order
tie-break (RTP2b1). The server-stamped `ID` also feeds the per-channel
idempotency index (§6) exactly as a message id does, so a resend of the same
`PRESENCE` frame (same connection, same `msgSerial`) dedupes per op.

`PresenceAction` matches Ably's wire enum: `ABSENT (0)`, `PRESENT (1)`,
`ENTER (2)`, `LEAVE (3)`, `UPDATE (4)`. A member's identity — its key in
the set — is `connectionId:clientId`, so the same `clientId` present
over two connections is two distinct members.

Messages and presence share **one ordered stream and one channelSerial
namespace**, so a single live list, a single Appender, and a single
NOTIFY path serve both. channelSerials are sortable cursors, not a dense
sequence, so the presence cms interleaved among data cms simply occupy
their own serials; a message-history scan skips them and a
presence-history scan skips data cms (the `kind` selector on
`storage.History`, §6). On the live linked list a subscriber's cursor
walks every cm and forwards each per its mode flags (§4.2): a
`SUBSCRIBE`-only attachment emits `MESSAGE` frames and ignores presence
cms; a `PRESENCE_SUBSCRIBE`-only attachment does the reverse.

The **membership set** is the fold of the presence stream: ENTER/UPDATE
establish or refresh a member (latest data wins), LEAVE removes it. The
server materialises this set (§12.5) so it can answer sync and
`GET .../presence` directly rather than re-deriving it from history on
every read.

### 12.2 Publishing presence (enter / update / leave)

An inbound `PRESENCE` frame carries `presence[]` of PresenceMessages with
action ENTER, UPDATE, or LEAVE, plus a `msgSerial` for flow control. The
connection:

1. Authorises — the attachment must hold the `PRESENCE` mode flag (which
   required the `presence` capability at attach time, §3.1); otherwise
   `NACK`.
2. Resolves and validates `clientId` (§12.3); a bad clientId → `NACK`.
3. Stamps `connectionId` and, for each op lacking a client-supplied `id`,
   the genuine-op `id` `<connectionId>:<msgSerial>:<index>` (§12.1), then
   calls `channel.StorePresence(ctx, presence)`, which mints the
   channelSerial, stamps each `PresenceMessage.serial`, persists the
   presence cm onto the stream, folds it into the membership set, and — via
   the Appender — links it onto the live list. Every `PresenceMessage.id`
   (server-stamped or client-supplied) is honoured for idempotency on the
   same per-channel index as message `id`s (§6), so a resent frame dedupes.
4. Replies `ACK` / `NACK` on the `msgSerial`, exactly as for a data
   publish (§5.2).

Subscribers with `PRESENCE_SUBSCRIBE` observe the event as an outbound
`PRESENCE` frame delivered by their attachment cursor, identically to
how `MESSAGE` frames are delivered (§4.4).

### 12.3 Client identity

A presence member must be identified, so presence requires a concrete
`clientId`. The rules extend §3.2:

- The PresenceMessage's `clientId` must equal the connection's resolved
  `clientId`. A connection with a concrete resolved clientId may omit it
  on the frame (the server stamps its own); supplying a *different* one
  → `NACK`.
- A connection whose token asserts the `*` (wildcard) clientId may enter
  any concrete clientId but must supply one — `*` is never itself a
  member identity (§3.2).
- A connection with no resolved clientId (anonymous) cannot enter
  presence → `NACK` (`code: 91000`).

`connectionId` is always stamped by the server and cannot be set by the
client.

### 12.4 Sync

When a client attaches with `PRESENCE_SUBSCRIBE` to a channel whose
membership set is non-empty, the server sets the `HAS_PRESENCE` flag on
`ATTACHED` and then delivers the current set as one or more `SYNC`
frames before resuming live delivery:

```
client                         server
  │ ── ATTACH(flags incl. PRESENCE_SUBSCRIBE) ──▶
  │   ◀── ATTACHED(flags incl. HAS_PRESENCE) ───│
  │   ◀── SYNC(presence[], channelSerial="<s>:<cursor>") ─│  × pages
  │   ◀── SYNC(presence[], channelSerial="<s>:") ────────│  final (empty cursor)
  │   ◀── PRESENCE … (live) ─────────────────────│
```

Each `SYNC` frame carries a page of members as PresenceMessages with
action `PRESENT`. The `channelSerial` field doubles as the sync cursor:
`<serial>:<cursor>` while pages follow, `<serial>:` (empty cursor part)
on the final page to mark completion. At the scale we target the set
usually fits a single frame; the cursor protocol allows paging for larger
sets.

Consistency between the snapshot and live delivery is resolved by the
**client's merge**, exactly as in Ably: every PresenceMessage carries a
serial-based `serial` and the SDK keeps the newest per member key. A member
that enters or leaves in the window between the snapshot's as-of serial
and the live attach point arrives again on the cursor — a duplicate
ENTER is idempotent and a later LEAVE supersedes a stale PRESENT — so no
server-side coordination beyond taking the snapshot at-or-after the
attach point is required. The `SYNC` alone must be complete as of the
attach point: an SDK takes the set as final once the sync ends
(`presence.get()` returns it), so operations the snapshot misses cannot
follow in a later frame.

**Where the snapshot comes from.** A node serves `SYNC` from its own copy
of the channel's member set, held by the node's `core.Channel` (§5.1) as a cache
of the store's set as of a serial:

- It is **seeded** from `Members` the first time a `SYNC` needs it after
  the channel is bound: one store read per bind, shared by concurrent
  attaches. `Members` returns the set and its as-of serial from one
  snapshot of the store, so the set is exactly the fold of every cm up to
  that serial. The cms the node delivers while the read is in flight are
  buffered and folded on top: those at or below the as-of serial are
  already in the seed and are skipped, those after it are applied.
- It is then **maintained** from every presence cm the node delivers on
  the channel, which arrive in channelSerial order (§7.2) and include
  other nodes' operations and the reaper's synthesised LEAVEs: ENTER,
  UPDATE and PRESENT upsert the member, LEAVE and ABSENT remove it. A cm
  at or below the set's serial (delivered twice) is skipped, and so is an
  operation older, by member serial, than the member's current state
  (last writer wins, the rule the SDK merge applies).
- The encoded `SYNC` frame is **cached**: the snapshot is built once and
  encoded once per wire format, then shared by every attach until a
  presence cm changes the set. It is rebuilt at most about once per
  refresh window (50 ms) while members keep changing: an attach that
  finds the snapshot out of date but younger than the window waits out
  the rest of it, then takes any snapshot built since it began (one
  rebuild serves every attach that waited), or builds one. An attach
  therefore waits at most one window however fast members change. Any
  snapshot built after the attach's stream was opened is at or after the
  attach point. A client-initiated `SYNC` (RTP19) is answered on the
  connection's read loop, so it never waits: an out-of-date snapshot is
  rebuilt at once.
- Eviction (§5.1) drops the set with the channel; a rebind seeds afresh.
  The set is also dropped when the bus sweep (§7.2) finds the channel with no attachment and no member of
  this node's, since the sweep no longer repairs a cm lost on the bus
  for it; the next attach seeds again. So a channel seeds on its first
  presence `SYNC` after a bind, and again on the first one after a sweep
  that found it with no subscribers.
  If the seed read fails, that `SYNC` is read from the store and the next
  one tries to seed again. If the backend cannot prove it delivered every
  cm (a gap the log no longer holds, or a catch-up past the retention
  window after the node was off the bus, on any bus including
  `pgnotify`; §7.2), it tells the channel, which drops the set so the
  next `SYNC` seeds again; a seed read in flight at that moment is
  discarded. Each `PRESENCE_SUBSCRIBE` attachment is then sent the
  re-seeded set after its channel update, as on attach. The memory and
  disk backends deliver each presence cm under the lock that mints it,
  so their cms also arrive in serial order.

The store's set stays authoritative (§12.5): `GET .../presence` and the
delayed-LEAVE checks always read the store.
Series: `ably_presence_syncs_total{snapshot}`,
`ably_presence_sync_seeds_total` (§10).

### 12.5 Membership set & liveness

The membership set is owned by the **storage backend**, alongside serial
state and the idempotency index — `StorePresence` folds each operation
into it transactionally and `Members` reads it (§6). Per backend:

- **memory** — a `map[memberKey]*PresenceMessage` per channel.
- **disk (bbolt)** — held in memory, *not* persisted. Presence is
  connection-scoped and no connection survives a process restart (§4.3),
  so the set is correctly empty on boot. Presence *history* still
  persists, as ordinary cms on the messages stream.
- **cluster (Postgres)** — the `presence` table (§6.3), upserted/deleted
  in the same transaction as the stream insert so the set is globally
  authoritative across nodes. `Members` reads it and the channel's
  watermark in one `REPEATABLE READ` snapshot (one round trip), so the set
  is exact as of the serial it returns. `GET .../presence` is served
  straight from `Members`; a node need never have witnessed the original
  ENTERs. `SYNC` is served from the node's member set, seeded from
  `Members` once per channel bind and maintained from delivered cms
  (§12.4); it is a cache of this table, never a second source of truth.
  Presence writes join the publish lanes' batches (§6.3 "Presence in a
  batch"), so one room's row lock is taken once per batch rather than
  once per ENTER or LEAVE. A presence write committed in its own
  transaction (every one with `--publish-lanes=0`; with lanes, a
  lease-lapse re-entry or a server-synthesised LEAVE written around a
  full lane) holds a pool connection for the whole transaction,
  including any wait on the room's row lock, so those writes are bounded
  per database (`--presence-max-inflight`, default 4 x `--publish-lanes`,
  16 when batching is off; server-synthesised LEAVEs are exempt (in the
  lanes they have a bound of their own, §6.3), so a mass disconnect with
  batching off can still hold
  many pool connections with LEAVEs, as before the bound existed): one
  beyond the bound is refused at once with
  Ably error **42910** (a NACK; the client should back off and retry) and
  counted in `ably_publish_nacks_total{reason="presence_inflight"}`, so a
  convoy on one hot room cannot starve the pool that `SYNC` seeds, binds
  and publishes need. Every retriable presence write failure NACKs with
  its code, as a publish does (42910, 50003).

**Static fixture members.** Members seeded from the config file's
`[[channels]]` presence entries (§9) are the one exception to
connection-scoped liveness: they belong to no
connection, so no teardown ever synthesises a LEAVE for them, and in
cluster mode they are stored with a sentinel owner and a non-expiring
(`'infinity'`) lease so neither the lease-bump loop nor the reaper ever
touches them. They persist for the process's lifetime. This exists only
for SDK test-suite compatibility.

**Liveness.** A member lives as long as the connection that entered it,
plus — for an *abrupt* disconnect — a short grace window,
`remainPresentFor` (default 15s, `DefaultRemainPresentFor`), before its
LEAVE is synthesised. The window exists so a client that resumes and
re-enters within it does not flicker out of and back into presence.
Departure:

- **Explicit LEAVE, DETACH, clean CLOSE, or graceful shutdown (§11)** — a
  deliberate departure, processed as an immediate LEAVE publish (no grace):
  the connection is not coming back, so there is nothing to wait for.
- **Abrupt disconnect (transport read error, heartbeat/token-expiry
  disconnect)** — when the connection loop exits it does *not* leave
  immediately. It hands the members it still holds (its own record of
  what it entered, with each member's last state; no store read) to the
  server, keyed by `connectionId`, for `remainPresentFor`. If a
  connection resumes that `connectionId` in the window (§4.3, §8), it
  takes the members over: it owns them from then on, so its own DETACH,
  CLOSE or drop leaves them, and no LEAVE is written for the dropped
  connection. Otherwise, at the end of the window, one LEAVE per channel
  is published through `StorePresence`; if a connection with that
  `connectionId` is live by then (it resumed while the dropped one was
  still tearing down) it takes the members over instead. A second
  abrupt drop of the same `connectionId` in the window merges its
  members into the pending entry and restarts the wait. A LEAVE that
  cannot be written (the channel cannot be bound, or the store refuses
  it) is logged at Warn and counted in
  `ably_presence_grace_leave_errors_total{stage}` (`get_channel`,
  `publish`); in cluster mode the member then stays until its node's
  lease ends. A graceful shutdown abandons any *already* pending delayed
  LEAVEs (the node is departing).

  The registry is the whole answer because a resume only ever reaches the
  node that issued the connectionKey: the key's HMAC secret is per process
  (§8), so a resume on another node gets a fresh `connectionId` and
  80018. An earlier version re-read the store at write time and compared
  member serials, to catch a resume that re-entered on another node; that
  case cannot occur, the comparison protected nothing the registry does
  not, and its store read could fail (it did, under load) and drop the
  LEAVE.

Because the server holds no other per-connection state across disconnects
(§4.3), this grace is self-contained — it is the *only* thing kept alive
for a dropped connection, independent of connection-state resume (still a
non-goal). A resume keeps its members whether or not the client re-enters;
a reconnect under a *new* `connectionId` (e.g. from `suspended`) leaves the
old member to age out over the window while the client re-enters afresh.

**Crashed cluster nodes.** A node that dies without running teardown
leaves orphaned rows in the `presence` table — the one case the LEAVE
path cannot cover, and a cluster-only one (a single-process crash takes
the whole set down with it). Each `presence` row therefore records its
owning `node_id`, stamped by `StorePresence` on ENTER/UPDATE, and the
owner holds a liveness lease. The storage backend runs two background
loops on fixed cadences (constants; operator config is a follow-up): a
**lease-bump** loop renews the lease every 10s, well inside the 30s
lease window, and a **reaper** loop runs every 5s on every node and
removes the members of dead nodes, synthesising a LEAVE for each through
the normal publish path (a fresh presence publish, announced on the bus
to every node's appender). Postgres row locking means exactly one
node's `DELETE ... RETURNING` yields a given row, so each LEAVE is
published once. The lease is one per node, a row in `presence_nodes
(node_id, expires_at)`:

- The bump is one upsert of that row, so its cost
  does not grow with the node's members and it never writes or locks a
  member row; a member row's own `expires_at` is `'infinity'`. A node is
  **alive** while its row exists with `expires_at` no more than one bump
  interval in the past (`expires_at >= now() - 10s`: the margin absorbs
  clock skew between nodes and a bump that is late by a tick), and dead
  otherwise. The reaper lists the node ids that own members (a skip scan
  over `presence_node_idx`, one index probe per distinct owner) with no
  live lease, plus the dead lease rows, and for each deletes the
  node's members in chunks of 1000, re-checking the lease in every chunk
  so a node that renews part way stops being reaped; once every chunk is
  deleted it publishes their LEAVEs, then deletes the node's lease row
  once it owns no member.
- The retired member lease mode (§9 "Removed settings") instead put a
  lease on every member row, renewed by one `UPDATE` of all of the
  node's rows per bump: at 100k members per node that rewrote and
  row-locked 100k rows every 10 s, which convoyed with presence batches
  writing the same rows (§6.3).

*Node identity.* A node id is random per process (40 bits, minted at
`postgres.Open`; a sharded node uses one id on every shard and keeps a
lease row in each shard's `presence_nodes`, beside the members it owns
there, §6.4). A restart is therefore a new node: no connection survives
a restart (§4.3), so no member of the old process can still be live, and
the old id's leftover rows are reaped. `Open` takes the lease before any
presence write can run, so a node's members never exist without its
lease. A graceful shutdown (§11) ends the lease after its connections'
LEAVEs: it deletes the row, or, if the node still owns members (delayed
LEAVEs the shutdown abandoned), marks it expired, so the next reaper round
on any node removes them within one reaper interval rather than a lease
window. If an id were reused (tests set it), `Open` re-takes the lease
with an upsert before serving, so the reaper fires on the old process's
members only if the gap between the two processes exceeded the lease
window.

*Reaper LEAVEs.* The LEAVEs of one channel's reaped members are one
presence publish, written in a transaction of its own (never batched),
up to 8 channels at a time; a dead node with members on 100k channels
therefore takes 100k transactions to announce (not measured at that
scale). Once that transaction holds the channel's
row lock it reads which of the members are in the `presence` table
again and leaves those out, storing nothing if none is left. Every
writer of a room's presence rows holds that lock first, so this sees any
ENTER committed before it, and an ENTER committed after it sorts after
the LEAVE: a node that re-enters its members after a lapse (below) is
never undone by a LEAVE the reaper had not yet published.

*Bound.* A member is never reaped while its node is alive: the reaper
deletes only rows whose node has no live lease, checked in the
deleting statement. A dead node's members are deleted within the lease
window after its last renewal plus one bump interval plus one reaper
interval (30s + 10s + 5s) plus the
chunked deletes themselves (about 0.7s for 100k members in a local test
on a 1M-row table), then their LEAVEs follow at publish throughput.
Several reaping nodes take different chunks of one dead node at once
(`SKIP LOCKED`); each deletes and announces only its own. A reaper that
stops after deleting rows and before publishing their LEAVEs loses those
LEAVEs. Every reaper statement runs under a
`statement_timeout` of half a bump interval: a chunk `DELETE` checks its
node's lease as of the moment it started, so one that started just
before the node renewed must not run on into the node's re-entry (below),
which starts a bump interval after the renewal. A statement that times
out deletes nothing and is retried next round.

*Reaper guard.* Every node's lease lives in the same database, so an
outage of that database longer than the lease window (a failover, a
partition of every node from it) lapses every lease at once, and the
first reaper to run after recovery would find every live node dead and
publish a LEAVE for every connected member. So a node reaps only while
it has itself renewed its lease without a break for at least one lease
window, and while its own last renewal is less than a window old. A
failed renewal, a renewal that finds the lease had lapsed, and `Open`
each start the wait again. After an outage every live node renews within
one bump interval, well inside the window every reaper waits, so none
of them is reaped; a node that died meanwhile is reaped one window after
the reapers came back. A deferred round counts in
`ably_presence_reaps_deferred_total` and logs at Info, at most once per
lease window, with the reason.

*Lapse and re-entry.* A node that could not renew for longer than the
window while other nodes could (a stall, a partition of that node
alone) may have had its members reaped and their LEAVEs published, while
its connections are still open. Its next renewal finds the lapse (the
row had to be re-created, or more than a window passed from the start of
the previous renewal to the completion of this one: the database stamps
a renewal somewhere in between, so a renewal that waited for a pool
connection or a slow network is caught; at worst a lapse is reported
that did not happen), logs a warning, counts it in
`ably_presence_lease_lapses_total`, and calls the storage's lapse hook
(`postgres.Options.OnPresenceLeaseLapse`) one bump interval later: long
enough for a reaper statement that was already running to finish, and
for the other shards of a sharded node, which share one hook and renew
on their own ticks, to report the same outage, so it runs once. The
server sets the hook to re-enter the node's members (*Re-entry*). A
lapse after an outage of the whole database finds no member reaped (the
guard); the node cannot tell, so it re-enters anyway, but the re-entry
writes only members missing from the table and publishes nothing for the
rest. It still costs one transaction per connection and channel, each
taking the channel's row lock and then rolling back, so after an outage
of the whole database every node adds that many row locks on its rooms
to the recovering primary's load for a while (not measured).

*Re-entry.* `realtime.Server.ReenterPresence` publishes one
server-synthesised ENTER per channel for every member each live
connection holds, with the member's last data, encoding and extras (each
connection keeps a copy of its members' last ENTER, UPDATE or PRESENT for
this), and the same for the members held for a dropped connection's grace
window (a resume would take them over expecting them present; they still
leave at the window's end; a grace LEAVE already written, or members a
resume has since taken over, are skipped, so this cannot undo the resumed
client's own LEAVE). It works on 32 connections or grace entries at a
time. Each ENTER is
written in a transaction of its own, after the publishes of its channel
already queued or in flight on the lane, and leaves out every member that
is in the table once the channel's row lock is held (the read waits for a
reaper delete of the row that has not committed yet). Each connection's
re-entry holds the connection's presence lock from reading its members
until the ENTERs are stored, and the client's own presence writes,
DETACH and teardown take the same lock, so a LEAVE the client sends
meanwhile is stored after the re-entry, never undone by it. A re-entered
member's subscribers see a LEAVE (the reaper's) then an ENTER, and its
row is back with its data. Re-entries count in
`ably_presence_reentries_total` (members actually written); a failed one
is logged and leaves that member absent until the client next updates
it, and one whose connection's write does not finish within 5 s is
abandoned the same way.

*Upgrading from member lease mode.* A node of an earlier version that
ran the retired member lease mode has no `presence_nodes` row and keeps
a lease on each of its member rows. The reaper lists such a node as
having no live lease, but takes a row only if its own `expires_at` is
`'infinity'` or dead, so the node's live members are left alone during
a rolling upgrade and reaped, with their LEAVEs, once the node is gone
and their leases have been dead for a bump interval. Meanwhile the
reaper reads that node's rows every round.

The reaper skips rows a presence write holds locked (`FOR UPDATE SKIP
LOCKED`): that write is renewing or removing the row anyway, a row
skipped once is handled on the next round, well inside the lease window,
and a reaper that waited could deadlock with a batched write holding
several members' rows (§6.3). The bump writes no member row. Static
fixture members (above) are owned by a sentinel that has no lease row
and carry an `'infinity'` lease; the reaper skips that owner.

### 12.6 REST

- `GET /channels/{channel}/presence` — the current membership set,
  served from `Members`; requires `subscribe`. Returns a PresenceMessage
  array (each with action `PRESENT`). Accepts `clientId` and
  `connectionId` exact-match filters (RSP3a2/RSP3a3) and paginates with
  `limit` and the same `Link` convention as message history (§2.2), the
  member `serial` doubling as the opaque page cursor.
- `GET /channels/{channel}/presence/history` — presence history, a
  `kind=presence` history scan (§12.1) paginated with the same `Link`
  convention as message history (§2.2); requires `history`.

There is no REST *write* surface for presence: a member is inherently
bound to a realtime connection, so entering presence is realtime-only.

## 13. Mutable messages

Messages on a channel can be **updated**, **deleted**, and **appended**
to after they are published. Like presence (§12), this is modelled as
operations on the append-only stream rather than mutation of stored
state: a mutation is a fresh publish — a new ChannelMessage that
references a prior message's `serial` and carries an `action` — plus a
derived **latest-version** view the server materialises so reads and
history can resolve the current state of each message without replaying
its whole version chain.

### 13.1 Model — identity, version, action

Every Message carries an `action` and a stable `serial` (§8):

| `action` | meaning |
|---|---|
| `create` | an original publish (the default; what §2.1 / §7 already describe) |
| `update` | replace fields of an existing message with a new version |
| `delete` | soft-delete an existing message (a tombstone version) |
| `append` | concatenate onto an existing message's data (§13.3) |

`serial` names the **message**; `version` names a **single version** of
it (§8). A create mints both equal to its own `<channelSerial>:<idx>`.
An update/delete/append is an ordinary publish that lands at a *new*
channelSerial: its Message repeats the target's `serial`, sets the
`action`, and gets a fresh `version` (its own position). Versions sort
lexicographically, so newest-wins is a string comparison.

The action enum mirrors Ably's `MessageAction`, pinned to ably-go's
constants at implementation (`create = 0`, `update = 1`, `delete = 2`,
`append = 5`; `summary` / `meta` occupy other values). The wire shape
matches Ably: `serial`, `action`, and a `version` object
(`{serial, timestamp, clientId, description, metadata}`).

Mutations ride the **same stream and Append/NOTIFY path** as any publish
(§7): they are `kind = message` cms distinguished only by `action`
(presence stays `kind = presence`, §12.1). Nothing on the live linked
list or in storage is rewritten in place — a mutation is purely
additive, and the `serial`-vs-`version` split is what lets readers
collapse the chain.

### 13.2 Update & delete

An update or delete is published — REST `PATCH .../messages/{serial}`
(target serial in the path, `action` in the body), or a WS `MESSAGE`
frame carrying the `action` and the target `serial`. The server:

1. Authorises against `message-{update,delete}-{own,any}` (§3.1): `-own`
   requires the caller's resolved `clientId` to equal the target
   message's creator; `-any` waives it. The WS surface also still
   requires the `PUBLISH` attachment mode.
2. Resolves the target's current latest version from the latest-version
   fold (§13.4). A target that does not exist (never published, or aged
   out of retention) is rejected (`ERROR` / `NACK` on WS, `4xx` on REST).
3. Applies **shallow-mixin** semantics: only the fields supplied among
   `data`, `name`, `extras` replace the corresponding fields of the
   current version; unspecified fields are carried forward. The server
   persists the resulting **merged** Message as the new version, so
   subscribers and history always carry a complete message, never a diff.
4. Stamps operation metadata into `version` (timestamp, operating
   `clientId`, optional description/metadata), mints the new `version`,
   persists the cm on the stream, and updates the `serial → versions`
   index and the latest-version fold (§13.4). The **top-level
   `Message.timestamp` is left as the original create time** (§8) — only
   `version.timestamp` carries this operation's time.
5. ACKs / NACKs (WS) or responds (REST) as for any publish.

A `delete` is **soft**: it writes a tombstone version (the latest fold
marks the message deleted) but the message and all its versions remain
queryable via version history (§13.4). Subscribers receive the new
version as an ordinary outbound `MESSAGE` frame carrying `action: update`
/ `delete` and the unchanged `serial` — delivered by the same attachment
cursor as any message (§4.4), so a live subscriber sees the edit or
removal in stream order.

### 13.3 Append

`append` concatenates `data` onto the message's current latest version
(name / extras follow the same shallow-mixin replace as update). It
targets the high-frequency single-publisher case (e.g. streaming an LLM
token sequence onto one message), so its delivery is looser than update /
delete:

- The server maintains the **rolled-up** latest data for the message. An
  append is persisted and fanned out as a full `action: update` whose
  `data` is the aggregate so far, carrying the incremental append in
  `alt["delta-append"]` (an `action: append` message holding the new
  `data`, sharing the version). The delta repeats the message's identity —
  its `name`, `extras` and top-level create `timestamp`, carried forward
  from the create (name / extras replaced by the append's own when it
  supplies them, §13.2) — so a subscriber routes an append frame by `name`
  and `extras` exactly as the create; only its `data` is the incremental
  slice. A streaming publisher omits the `name` on each append (relying on
  this carry-forward), so a name-less delta would be invisible to a
  name-filtering subscriber. The delivery path chooses per subscriber:
  a subscriber that is caught up receives the delta **incrementally**
  (`action: append`, just the new `data`); the first delivery for a
  message a subscriber has not yet seen — e.g. immediately after attach,
  or backlog replay on resume/rewind — is the full `action: update`
  carrying the aggregate, after which it receives subsequent appends
  incrementally. This needs per-attachment tracking of which message
  identities the subscriber has seen since attach.
- That tracking is bounded so that it does not grow with every message
  a long-lived attachment receives. An attachment records a serial only
  when a later delta for it is possible: the delivered message carries
  an append, or the channel is in a namespace with `mutableMessages`
  (§9), where a create may be appended to later. Ordinary messages on
  other channels record nothing, and nothing is recorded under
  `appendMode=full`. The record holds at most `--attachment-seen-max`
  serials (default 4096) per attachment, in two generations of half that
  size; a serial is kept until at least half the cap of other serials has
  been recorded since it was last recorded, so a message that is still
  being appended to stays. Eviction is safe by construction: a serial the
  attachment does not hold is treated as not yet seen, so its next
  append is delivered as the full `action: update` aggregate, which is
  always a valid delivery under the conflation rule below. On a channel
  outside a `mutableMessages` namespace the first append after a create
  is therefore a full update, and later appends are deltas.
- The server may **conflate**: coalesce multiple appends, drop superseded
  intermediate versions, or deliver an append as a full rolled-up
  `update`. The only guarantee is that the last version a subscriber
  receives is the most recent — there is no promise that every
  intermediate append is delivered. Collapsing the pre-attach append run
  into a single full-on-first-sight update is exactly this: the
  intermediate appends a fresh subscriber never saw are never sent.
- Appends are **not** retained as individual entries in version history;
  only the aggregated latest version is durable (§13.4). The
  `appendMode=full` channel param opts a subscriber out of incremental
  delivery, so it always receives the full rolled-up versions instead of
  append deltas.

Append aggregation is inherently stateful and conflation-sensitive, which
is why its delivery contract is deliberately looser than that of update /
delete.

### 13.4 Reads & history

Two derived structures, both owned by the storage backend (§6) and
maintained transactionally with each version's persist into the
`channel_messages` log — the message analogues of the presence membership
set:

- **the materialised `messages` projection** (`serial → latest merged
  Message`, with a deleted tombstone). Backs `GET .../messages/{serial}`
  (one message, latest version) and the default `GET .../messages`
  history, which returns the **latest version of each message positioned
  at its create serial** — an edited message keeps its place in the
  timeline but shows current content, and a deleted message shows as a
  tombstone.
- **the `serial → versions` index** over the log. Backs
  `GET .../messages/{serial}/versions`, which returns the versions of a
  message ordered by `version` — oldest-first (create then edits) by
  default, since a version chain reads naturally forwards and the SDK
  requests it without a direction; an explicit `direction` param still
  wins. Paginated with the same `Link` convention as message history
  (§2.2). It is also what an update / delete consults
  to validate and merge against its target. Appends are the exception to
  "every version": the log keeps each append cm for live and resume
  fan-out, but the versions read-path collapses a run of appends to its
  aggregate, so a streamed append shows as one evolving version rather
  than one entry per delta (§13.3).

Live and resume delivery are **not** collapsed: a fresh subscriber, and a
resuming one replaying the gap (§4.3), receive the raw version cms in
stream order (create, then each edit) and converge by newest-`version`
exactly as they would have live. Only history *reads* collapse to the
latest version. `rewind` (§4.3) counts stream cms, so a window may span
several versions of the same message.

### 13.5 Capabilities

Mutations add four capability ops to §3.1, matching Ably:
`message-update-own`, `message-update-any`, `message-delete-own`,
`message-delete-any` (append is gated by `message-update-*`). The `-own`
/ `-any` distinction is the first **ownership-scoped** op in the model:
`-own` resolves only if the caller's `clientId` (§3.2) equals the target
message's creator `clientId`; `-any` skips the check. Creating a message
is still plain `publish`; all version reads (latest, by-serial, versions)
use `history`.

### 13.6 Surface

- **WS** — inbound `MESSAGE` carries `action` + (for mutations) the
  target `serial`; outbound `MESSAGE` carries `action` + `version`. No
  new `ProtocolMessage` action is introduced — mutations reuse `MESSAGE`
  (15), distinguished by the Message-level `action` field.
- **REST** — `POST .../messages` creates; `PATCH .../messages/{serial}`
  carries a mutation (target serial in the path, `action` in the body);
  `GET .../messages/{serial}` and `GET .../messages/{serial}/versions`
  read the latest version and the full version chain (§2.2).

## 14. Annotations & summaries

An **annotation** is a piece of metadata a client attaches to an existing
message — a reaction, a citation, a flag — identified by a `type` and
aggregated by the server into a per-message **summary** that subscribers
and history readers consume instead of the raw annotation stream. As with
presence (§12) and mutations (§13), annotations are operations on the
append-only stream plus a derived view: nothing is rewritten in place.

### 14.1 Model

An annotation is a publish like any other: it lands on the channel as one
ChannelMessage carrying `Annotations []*protocol.Annotation` (never
`Messages` or `Presence`) and flows through the unified
`storage → Appender.Append` path (§7), as a third stream kind
(`kind = annotation`) sharing the channelSerial namespace.

An Annotation carries: `id` / `serial` with the same split as Message
(§8 — client-supplied idempotency key vs server-assigned
`<channelSerial>:<idx>`), an `action` (`annotation.create = 0`,
`annotation.delete = 1`), the target `messageSerial`, a `type`, optional
`name`, `count`, `data`/`encoding`, and the attributed `clientId` plus
server-stamped `connectionId` and `timestamp` — field names and enum
values pinned to Ably's wire shape.

The `type` has the form `<name>:<aggregation>` where `<aggregation>` is
one of the five v1 **summarisation methods**: `distinct.v1` (per-value
set of distinct clientIds), `unique.v1` (one value per clientId, newest
wins), `multiple.v1` (per-value counts, `count` honoured), `flag.v1`
(boolean per clientId), `total.v1` (anonymous tally). Validation mirrors
Ably: a missing/malformed `messageSerial` or `type` is rejected (40000).

**Identity.** Annotation publishes are attributed: a concrete `clientId`
is required (resolved per §3.2), except that the `multiple.v1` and
`total.v1` methods also accept anonymous publishes — mirroring Ably's
anonymous-aggregation allowance. An `annotation.delete` removes the
caller's own contribution(s) for the type, per the method's fold.

**Target existence.** The target message must resolve in the
latest-version projection (§13.4) — i.e. exist within retention — or the
publish is rejected, exactly as for update/delete.

### 14.2 Summary fold

The summary is the fold of a message's annotations, computed **by the
storage backend transactionally at store time** — the same pattern as the
presence membership set (§12.5) and the latest-version projection
(§13.4). `StoreAnnotation` mints the channelSerial, persists the
annotation cm on the log, folds it into the target message's summary (a
`map[type]aggregation` keyed by the full `<name>:<aggregation>` type),
and stamps the **post-fold summary snapshot onto the stored cm** before
it reaches the Appender.

That snapshot is what makes cluster delivery deterministic: every node —
including ones that never witnessed earlier annotations — emits the
summary for a given annotation cm from the cm itself, off the normal
Append/NOTIFY path (§7.2). No node ever publishes a separate rollup, so
there is no duplicate-summary problem and no cross-node coordination
beyond the existing per-channel advisory lock. There is no debounce:
one summary delivery per annotation, which Ably's conflation latitude
permits (only the *latest* summary a subscriber holds matters).

The summary is stored on the messages projection row (the merged latest
Message carries its `summary`), so it dies with the message under
whatever retention policy applies (§6) — a summary is message
state, not independent state.

### 14.3 Delivery

On Append of an annotation cm a node emits, per attachment mode (§4.2):

- **`ANNOTATION` (21)** — the raw annotation, only to attachments
  holding `ANNOTATION_SUBSCRIBE`. Publishing requires the
  `ANNOTATION_PUBLISH` mode on the attachment (WS) and is NACKed
  without it.
- **`MESSAGE` with `action: summary` (4)** — the post-fold summary
  snapshot, carrying the target message's unchanged `serial` and the
  `summary` object, to ordinary `SUBSCRIBE` attachments. This is how a
  subscriber that knows nothing about annotations sees reactions/
  citations accumulate. Wire shape:
  `{"action": 4, "serial": "<target>", "summary": {"<type>": {…}}}`
  with the per-method value shapes (`{value: {total, clientIds}}` for
  distinct/unique, counts for multiple, etc.) pinned to Ably's.

The two new mode flags extend the §4.2 table: `ANNOTATION_PUBLISH`
(`1 << 21`) and `ANNOTATION_SUBSCRIBE` (`1 << 22`), permitted iff the
capability grants `annotation-publish` / `annotation-subscribe`
respectively (§3.1). When `ATTACH.flags` carries no mode bits,
`ANNOTATION_PUBLISH` **is** part of the default set (matching the
reference's `MODE_DEFAULT` and the SDK's default `channel.modes`), but
`ANNOTATION_SUBSCRIBE` is **not**: raw annotation delivery must be opted
into via modes or channel params.

Both frames are a **live** delivery derived from the one annotation cm as
the attachment cursor walks it: the raw `ANNOTATION` goes to
`ANNOTATION_SUBSCRIBE` attachments, and the summary `MESSAGE` — built from
the snapshot the cm carries, never recomputed — to `SUBSCRIBE` ones. The
summary is not a separate persisted cm; it exists only on the projection
row (for reads) and as the snapshot stamped on the annotation cm (for
delivery). The resume gap replay (§4.3) is a `kind = message` history
scan, so — exactly as it skips presence — it skips annotation cms
entirely; neither the raw annotation nor its derived summary is replayed
as a frame. A resuming subscriber re-reads raw annotations via the REST
endpoint (§14.4) and reconverges on the current summary through the
projection-backed message reads (§14.4) — the newest summary always rides
the latest version of its message — and via any subsequent live
annotation. History *reads* do not enumerate annotation cms: they return
messages whose embedded `summary` is current, and a `kind = message`
history scan skips annotation cms exactly as it skips presence.

### 14.4 Reads

- `GET /channels/{channel}/messages/{serial}/annotations` — the raw
  annotations for one message, in stream order, paginated with the
  standard `Link` convention (§2.2); requires `history` (reads) —
  publishes via `POST` on the same path require `annotation-publish`.
- Message reads (`GET .../messages`, `GET .../messages/{serial}`,
  history) carry the current `summary` on each message via the
  latest-version projection — no separate summary endpoint.

Storage: annotation log rows are `kind = annotation` with
`message_serial` holding the **target** message serial, so the existing
serial index serves annotations-for-message scans (message-version scans
add `kind = 'message'` to their predicates, §6.3). The summary lives in
the messages projection payload; bbolt mirrors this in its `messages`
bucket value, memory in its projection map.

### 14.5 Capabilities

Two ops extend §3.1, matching Ably: `annotation-publish` (publish an
annotation, WS or REST) and `annotation-subscribe` (receive raw
`ANNOTATION` frames). Summaries require only `subscribe` — they are
message deliveries. Reading annotations via REST requires `history`.

## 15. Sandbox provisioner

The `ably-server` binary is strictly single-app: one app's keys, one
channel namespace. Ably's SDK test suites, however, provision a fresh app
per test run against a sandbox REST host (`POST /apps`), then point clients
at the returned keys. The `ably-local-sandbox` command bridges the two — a
disposable-instance-per-test-run provisioner.

`ably-local-sandbox` is a separate process that serves the harness-facing
provisioning API (default `:9080`) and spawns **one `ably-server` child
per provisioned app**, so the core server never grows multi-app
machinery. It keeps the core single-app while giving the harness the
multi-app surface it expects.

**`POST /apps`** accepts the Ably *test-app-setup* `post_apps` body — keys
(each with an optional capability, carried either as a stringified JSON
object or a nested object; unrecognised fields such as `revocableTokens`
are echoed back untouched), `namespaces`, and `channels` with presence
members. For each request the provisioner:

- generates an `appId`/`accountId` and, per key, an `appId.keyId:keySecret`
  triple in the exact format the SDKs parse (the random tokens use a
  base64url alphabet, so they never contain `.` or `:`);
- translates the body into a temporary TOML config expressing the
  `[[keys]]` (with capabilities), `[[namespaces]]`, and
  `[[channels]]`/presence sections (§9), emitted through the same
  `internal/config` types the server parses, so escaping round-trips;
- boots a child `ably-server --config <tmp> --mode memory --listen
  127.0.0.1:0 --addr-file <tmp> --enable-stats-stub` (the last flag turns
  on the child's normally-unregistered `/stats` stub, §1/§9, since compat
  suites depend on it), discovers the bound port from the addr-file
  (§9), and
- responds `201` with the sandbox app JSON — `appId`, `accountId`, `keys[]`
  (each carrying `keyName`, `keySecret`, `keyStr`, and the capability as a
  JSON string), the echoed `namespaces`/`channels`/`cipher` blocks —
  **extended** with `endpoint`/`port`/`tls` fields so the harness can route
  clients straight at the child. The response preserves the harness
  invariant that `keys` and `namespaces` count-match the request.

**`DELETE /apps/{appId}`** terminates that child (SIGTERM to its process
group, escalating to SIGKILL after a grace window) and is idempotent
(`204` whether or not the app is known). **`POST /stats`** accepts and
discards stats fixtures (`201`), mirroring the server's stub (§1): the
harness posts them at the provisioning host.

Children run in their own process group so a single signal reaches the
real server even under the `go run` fallback, and inherit an environment
with `ABLY_SERVER_*` stripped so a variable set for the provisioner can't
override the per-app config. Each child's stdout/stderr and its config go
to a per-app directory under a temp dir. The provisioner tracks
`appId → process + port + last-touched`; an idle-TTL reaper (default 30m)
kills children not provisioned or deleted within the window, and a SIGTERM
to the provisioner tears down every child before it exits. Concurrent
`POST /apps` calls produce independent children on distinct OS-assigned
ports with no shared state.

The provisioner finds the `ably-server` binary via `--server-bin`; absent
that, a sibling named `ably-server` next to its own executable; absent
that, it falls back to `go run ./cmd/ably-server` (which assumes the
working directory is the module root, the case when the provisioner itself
runs under `go run`).

**Cluster-mode children.** With `--child-postgres-dsn` the provisioner
runs every child in cluster mode instead (`--mode cluster
--postgres-dsn <dsn with search_path=<schema>> --bus <--child-bus>`, plus
`--nats-url <--child-nats-url>` for `--child-bus=nats`). Each app gets a
fresh schema in that database, created on `POST /apps` and dropped when
the child is terminated. A schema per app keeps apps apart in Postgres
and, because the bus hashes the schema into its channel names (§7.2),
on a shared NATS server too. CI uses this to run an SDK suite end to end
on the `nats` bus. The flags have `ABLY_LOCAL_SANDBOX_CHILD_POSTGRES_DSN`,
`ABLY_LOCAL_SANDBOX_CHILD_BUS` and `ABLY_LOCAL_SANDBOX_CHILD_NATS_URL`
equivalents. A comma-separated `--child-postgres-dsn` list gives sharded
children (§6.4): each app's schema is created in and dropped from every
database, and the child gets the list with the schema set on each DSN.

## 16. Testing strategy

- **Unit**: per-package; mock-free where practical (the storage interface
  has an in-memory implementation, exercised by the same test suite as the
  bbolt and Postgres backends — table-driven contract tests).
- **Integration**: spin up the binary against ably-go's existing test suite
  (or a curated subset) to validate SDK compatibility.
- **Cluster**: Postgres + 2 server processes in Docker Compose; tests cover
  cross-node publish and `channelSerial`-based replay on reconnect to a
  different node.
- **Per bus** (§7.2): the storage contract suite runs on every cluster bus
  (`pgnotify`, `postgres` coalesced and transactional, `nats`); the SDK
  integration suite runs on any of them via `ABLY_INTEGRATION_BUS`; each
  bus has a three-node compose stack under `bench/` with
  `bench/compose-smoke.sh`; and CI runs all of these as a matrix, plus the
  ably-go suite against cluster-mode sandbox children on the `nats` bus
  (§15). NATS-cluster failover is tested against three NATS servers.

There is no existing Ably protocol conformance suite to target; the
ably-go integration tests are the de-facto external check on SDK
compatibility.

## 17. Project layout & licensing

- License: **Apache 2.0** (matches ably-go).
- Module: `github.com/ably/ably-server`.
- Go version floor: **1.26**.
