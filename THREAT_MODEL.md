<!--
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache IoTDB — Threat Model (v0 draft)

## §1 Header

- **Project:** Apache IoTDB (`apache/iotdb`), `master` / 2.0.x line, against which this draft was written.
- **Date:** 2026-06-02. **Status:** draft — for IoTDB PMC review. **Author:** ASF Security team (drafted via the Scovetta threat-model rubric), for PMC ratification.
- **Version binding:** this model is versioned with the project; a report against IoTDB version *N* is triaged against the model as it stood at *N*, not at HEAD.
- **Reporting cross-reference:** findings that violate a §8 property should be reported privately per the ASF process (`security@apache.org` → `private@iotdb.apache.org`); findings that fall under §3 or §9 will be closed citing this document.
- **Provenance legend:** *(documented)* = stated in IoTDB's own docs/repo; *(maintainer)* = confirmed by an IoTDB PMC member through this process; *(inferred)* = reasoned from architecture/domain knowledge, not yet confirmed — every *(inferred)* claim has a matching §14 open question.
- **Draft confidence:** ~14 documented / 0 maintainer / ~41 inferred. This is a v0: most trust-boundary and adversary claims are hypotheses for the PMC to confirm or correct.
- **What IoTDB is:** Apache IoTDB is a time-series database management system for IoT data — collection, storage, query, and analysis. It runs as a server (standalone, or a distributed cluster of ConfigNodes for metadata/coordination and DataNodes for storage/query), stores data in the TsFile columnar format, and is accessed over a Thrift-based RPC protocol via a SQL-like language and JDBC/session clients. *(documented — README, repo `CLAUDE.md`)*

## §2 Scope and intended use

- **Primary use:** an operator-deployed time-series database server, written to by IoT/device data pipelines and read by analytics clients, over the network. *(documented — README)*
- **Deployment shapes:** standalone single-node, and a distributed cluster (ConfigNode + DataNode roles, inter-node consensus). *(documented — repo `CLAUDE.md`)*
- **Caller roles** (this is a network service, not an in-process library — the "caller" splits):
  - **client** — connects over the Thrift session protocol / JDBC / SQL; authenticates with a username/password and is constrained by RBAC privileges. Treated as **untrusted** beyond its granted privileges. *(inferred)*
  - **operator/admin** — the `root` superuser and whoever controls the deployment, configs, and the host. **Trusted** for the instance. *(inferred)*
  - **peer node** — another ConfigNode/DataNode in the same cluster, authenticated into the cluster. Trusted to the extent the cluster security model trusts members. *(inferred)*

**Component-family table** *(in/out = in/out of this model; all rows inferred unless noted)*:

| Family | Entry point | Touches outside process | In model? |
| --- | --- | --- | --- |
| Client RPC / session + SQL/query engine | Thrift session protocol (`iotdb-protocol/thrift-datanode`), JDBC, SQL | network (listens), filesystem (TsFile) | **In** *(documented: Thrift modules exist)* |
| Authentication / RBAC | login + privilege checks (Users/Roles/Privileges) | — | **In** *(documented)* |
| Cluster control + consensus | ConfigNode RPC, inter-node consensus (`thrift-confignode`, `thrift-consensus`) | network (inter-node) | **In** *(documented: modules exist; trust posture inferred)* |
| Extension / server-side execution | UDF (`USE_UDF`), Triggers (`USE_TRIGGER`), Pipe (`USE_PIPE`), Models/AINode (`USE_MODEL`), templates | runs user-supplied logic/JARs; Pipe opens network | **In, but see §9** *(documented: privileges exist; execution semantics inferred)* |
| REST API / MQTT ingestion | HTTP REST service, MQTT broker (if enabled) | network (listens) | **In if enabled** *(inferred — confirm which protocols ship enabled)* |
| TsFile on-disk format | `apache/tsfile` (separate repo) | filesystem | **Out** — separate repo, model separately *(documented: TsFile is a separate project)* |
| Client SDKs | `iotdb-client-{go,nodejs,csharp}` (separate repos) | — | **Out of this batch** — deferred to later submissions *(per engagement scope)* |
| `example/`, `integration-test/` | demo + test code | — | **Out** *(see §3)* |

## §3 Out of scope (explicit non-goals)

- **Attackers who already control the host or the IoTDB process / config files / data directory.** They have the operator's authority by definition. *(inferred)*
- **`example/`, `integration-test/`, build/distribution tooling.** Shipped in the repo but not a production trust surface; threat-model separately if ever promoted. *(inferred)*
- **TsFile format internals** — owned by `apache/tsfile`; a parsing/decoding finding in TsFile is routed there, not here (this model covers IoTDB's *use* of TsFile, not the format library). *(inferred — confirm the boundary)*
- **The client SDK repos** (`iotdb-client-go/nodejs/csharp`) — out of this scan batch by agreement; each is enrolled separately as its discoverability lands. *(documented — engagement scope)*
- **Confidentiality of data at rest / in transit when the operator has not enabled encryption** — see §10; TLS/disk-encryption posture is the operator's deployment responsibility unless the project claims otherwise. *(inferred)*

## §4 Trust boundaries and data flow

- **Primary trust boundary: the authenticated client RPC surface.** Bytes arriving over the Thrift session protocol (and any REST/MQTT endpoint) from a client are untrusted; a client is constrained to its RBAC-granted privileges. The query engine, schema engine, and storage layer sit behind this boundary. *(inferred)*
- **Secondary boundary: the cluster/inter-node surface.** ConfigNode↔DataNode RPC and the consensus channel — whether this is assumed to run on a trusted private network, or is itself authenticated/encrypted against an active network attacker, is the single biggest open question (see §14). *(inferred)*
- **Reachability preconditions per component** (the test a triager applies before anything else):
  - A finding in the query/SQL/schema engine is **in-model** only if reachable from a client operating *within its granted privileges* (or from an unauthenticated pre-login surface). *(inferred)*
  - A finding requiring an already-`root`/admin session is **out-of-model: trusted-input** unless it crosses into host compromise the operator didn't already have. *(inferred)*
  - A finding in UDF/Trigger/Pipe/Model execution is in-model only subject to the §9 ruling on whether running privileged user-supplied code is a boundary or by-design. *(inferred)*
  - A finding on the inter-node channel is in-model only if the cluster threat posture (§14) treats that channel as exposed. *(inferred)*

## §5 Assumptions about the environment

- **Runtime:** JVM, Java 1.8+ (1.8–25 verified). *(documented — README)*
- **OS:** Windows / macOS / Linux. *(documented — README)*
- **Filesystem:** the data/WAL/TsFile directories are private to the IoTDB process and not writable by untrusted local users; `max open files` raised to 65535. *(documented for the fd limit; filesystem-privacy inferred)*
- **Concurrency:** the server is multi-threaded and serves concurrent client sessions; thread-safety of the storage/query path is a correctness assumption. *(inferred)*
- **Clock:** time-series semantics depend on timestamps; whether server-side time ordering assumes monotonic/synchronized clocks across the cluster is open. *(inferred)*
- **What the server does to its host** (negative inventory — predominantly inferred, a wave-1/2 confirmation target): listens on network ports; reads/writes its data + WAL directories; reads its config files; spawns no child processes *except* where features explicitly do (e.g. Pipe sinks, external scripts?); the UDF/Trigger/Model features load and execute user-supplied code in-process. *(inferred)*

## §5a Build-time and configuration variants

Knobs that change which security properties hold *(all inferred — confirm the list and the defaults)*:

- **Authentication enabled / default `root:root`** — IoTDB ships a single fixed administrator `root` with password `root`. *(documented — Authority-Management docs)* Whether shipping with the default password unchanged is a **supported production posture** (a report against it = `VALID`) or a documented must-change (a report = `OUT-OF-MODEL: non-default-build`) is a **wave-1** ruling (see §14).
- **TLS / wire encryption** for client and inter-node channels — default on or off? *(inferred)*
- **REST API / MQTT** — shipped enabled or opt-in? *(inferred)*
- **UDF / Trigger / Pipe / AINode-model** execution — enabled by default, and gated only by privilege? *(inferred)*
- **Whitelist / network bind** (bind address, client allow-list) defaults. *(inferred)*

## §6 Assumptions about inputs

Per-surface trust table *(all inferred unless noted; confirm sinks + which protocols ship enabled)*:

| Surface | Input | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| Thrift session `openSession` / login | username, password | **yes** (pre-auth) | strong `root` password; lock-out / rate-limit posture |
| Thrift session execute (SQL / inserts) | SQL text, tablet/row payloads, paths | **yes**, within granted privileges | privilege grants; query resource limits |
| REST API (if enabled) | HTTP body, headers, auth | **yes** | TLS, auth, network exposure |
| MQTT (if enabled) | topic, payload | **yes** | auth, network exposure |
| UDF / Trigger / Model registration | JAR / class / model artifact | **yes if a non-admin privilege grants it**; else admin-only | who may register executable extensions |
| Inter-node RPC / consensus | peer messages | **yes if the cluster network is exposed** | trusted network or mutual auth |
| Config files, JVM flags, data dir | local | no — operator-trusted | filesystem permissions |

- **Shape/rate:** whether the server bounds per-query CPU/memory, result-set size, or concurrent sessions — and whether exceeding those is a bug or expected-degradation — is open (see §8 resource line). *(inferred)*

## §7 Adversary model

- **Primary adversary:** a network client that can reach the IoTDB RPC/REST/MQTT port — either **unauthenticated** (pre-login) or **authenticated with limited privileges** — trying to read/write data outside its grants, escalate privilege, execute code on the server, crash/exhaust the server, or move laterally to peer nodes. *(inferred)*
- **Capabilities assumed:** can open connections, send arbitrary protocol/SQL bytes, supply large/malformed payloads, and (if granted) register extensions. *(inferred)*
- **Out of scope:** anyone with `root`/admin session or host/process/filesystem control (already authoritative); side-channel/timing adversaries (unless the PMC wants them in). *(inferred)*
- **Cluster — authenticated-but-Byzantine peer:** a node that holds a valid cluster identity and then behaves arbitrarily. Whether IoTDB's consensus claims safety/liveness against such a peer (and under what honest-fraction threshold), or whether cluster membership is assumed fully trusted, is open (see §14). *(inferred)*

## §8 Security properties the project provides

*(All inferred pending PMC confirmation — a property only counts once the project commits to it.)*

- **Authentication + RBAC enforcement.** A client cannot read/write series data or schema, or perform global operations, beyond the privileges granted to its user/roles; unauthenticated clients cannot act. *Violation symptom:* an unprivileged session reads/writes data or runs admin operations. *Severity:* security-critical (auth bypass / privilege escalation). *(inferred — the RBAC model is documented; the guarantee that it is unbypassable is the claim to confirm)*
- **Memory/availability safety on the pre-auth + client RPC surface.** Malformed Thrift/SQL/REST input yields a clean error, not a crash, OOM, or hang of the server. *Violation symptom:* server crash / unbounded allocation / deadlock from client input. *Severity:* security-critical (remote DoS) if pre-auth; lower if it requires privilege. *(inferred — confirm whether any resource guarantee is made; see resource line)*
- **Tenant/path isolation.** A client with privilege on one path/database cannot read or write series outside its granted paths. *Violation symptom:* cross-path data access. *Severity:* security-critical. *(inferred)*
- **Resource bounds — UNSPECIFIED.** Whether a single expensive query (huge scan/aggregation) or a flood of writes that exhausts CPU/memory/disk is a **bug** or **expected and the operator's capacity-planning problem** is open. The model needs a categorical line here (see §14); until then DoS triage is undefined. *(inferred)*

## §9 Security properties the project does *not* provide

*(The highest-value section for integrators — all inferred, confirm each.)*

- **Server-side code execution via extensions is likely by-design, not a vulnerability.** UDFs, Triggers, Pipe processors, and AINode models execute user-supplied logic/JARs **inside the server process**. If the privilege to register them is held, code execution on the server is the *intended* behaviour — analogous to "a DB admin can run code." A scan reporting "UDF/Trigger allows arbitrary code execution" is then `BY-DESIGN`, **provided** registration is gated to a sufficiently-trusted privilege. The exact gating (admin-only vs. a grantable privilege) decides where the line sits. *(inferred — this is the single most important ruling; see §14)*
- **Transport confidentiality/integrity is the operator's job** unless TLS is enabled — IoTDB does not, by default, defend against a network attacker reading/modifying client or inter-node traffic. *(inferred)*
- **No defense against a malicious operator / `root`.** *(inferred)*
- **Default-credential exposure is not IoTDB's bug if the operator left `root:root`** — pending the §5a ruling. *(inferred)*
- **False friends:** RBAC privileges are an *authorization* mechanism, not a *sandbox* — granting `USE_UDF` is not a safe boundary against hostile code (it is a grant of code execution). The username/password login is not a defense against a network MITM without TLS. *(inferred)*
- **Well-known classes left to the caller:** SQL-injection-style abuse via clients that build IoTDB SQL from their *own* untrusted input (the embedding app's responsibility); resource-exhaustion via expensive queries (see §8); credential-stuffing against the login surface. *(inferred)*

## §10 Downstream responsibilities (operator/deployer)

*(All inferred — confirm.)*

- Change the default `root` password before exposing the server. *(inferred)*
- Do not expose the client RPC/REST/MQTT ports directly to an untrusted network; place IoTDB behind a trusted network boundary unless TLS + auth are configured. *(inferred)*
- Run the inter-node cluster channel on a trusted/segmented network (or enable mutual auth/TLS if supported). *(inferred)*
- Restrict who is granted extension privileges (`USE_UDF`/`USE_TRIGGER`/`USE_PIPE`/`USE_MODEL`) — they are equivalent to server code execution. *(inferred)*
- Set filesystem permissions so only the IoTDB user can read the data/WAL/config directories. *(inferred)*
- Apply per-query / per-session resource limits appropriate to capacity. *(inferred)*

## §11 Known misuse patterns

*(Draft one-liners — expand before publishing.)*

- Exposing IoTDB directly to the public internet with default credentials. *(inferred)*
- Granting extension privileges to semi-trusted clients, treating RBAC as a sandbox. *(inferred)*
- Building IoTDB SQL by string-concatenating the embedding application's untrusted input. *(inferred)*
- Running a cluster across an untrusted network without inter-node protection. *(inferred)*

## §11a Known non-findings (recurring false positives)

*(Seed list — all inferred; the PMC's confirmations here are the highest-leverage suppression input for the scan.)*

- "UDF/Trigger/Pipe/Model can run arbitrary code" — by-design server-side execution gated by privilege (§9), not a finding, subject to the §14 gating ruling.
- "Default password is `root`" — operator must-change per §10/§5a (disposition depends on the §5a ruling), not a code bug in itself.
- "No TLS by default on the wire" — operator deployment responsibility (§9/§10), unless the project claims default transport security.
- "Admin/`root` can do destructive operation X" — out-of-model: the admin is trusted (§7).
- "Expensive query consumes lots of CPU/memory" — pending the §8 resource line; likely expected unless super-linear/unbounded beyond a stated threshold.

## §12 Conditions that would change this model

- A new client-facing protocol or endpoint; a change to the default auth posture (e.g. `root:root` → forced change), default TLS, or default-enabled REST/MQTT. *(inferred)*
- Promoting a client SDK or `example/` code into the production trust surface. *(inferred)*
- A change to the cluster trust/consensus posture. *(inferred)*
- **A report that cannot be routed to a single §13 disposition** — signals a model gap; revise the model rather than make an ad-hoc call.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope adversary/input (auth bypass, cross-path access, pre-auth crash/OOM, privilege escalation). | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but the API/SQL makes a §11 misuse easy enough to harden. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires an admin/`root` session or operator-controlled config/files. | §6, §7 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires a capability the model excludes (host control, side channel, exposed inter-node net when posture says trusted). | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `example/`, `integration-test/`, or the separate TsFile / SDK repos. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests under a discouraged/non-default §5a setting. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (extension code execution within its gate, no-TLS-by-default, malicious operator). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `MODEL-GAP` | Cannot be cleanly routed — triggers a §12 revision. | §12 |

## §14 Open questions for the maintainers

**Wave 1 — scope & intended posture** (reshapes everything):
1. **Deployment posture:** is IoTDB intended to be exposed directly to untrusted clients/networks, or deployed inside a trusted network behind the application tier? Proposed: trusted-network-by-default, with the client RPC surface as the in-model boundary. → §2/§4/§7.
2. **Default `root:root`:** supported production posture (report = `VALID`) or documented must-change (report = `OUT-OF-MODEL: non-default-build`)? Proposed: must-change, operator responsibility. → §5a/§10/§11a.
3. **TsFile boundary + SDK repos:** confirm TsFile findings route to `apache/tsfile`, and the `iotdb-client-*` SDKs are out of this batch. → §2/§3.

**Wave 2 — trust boundaries & protocols:**
4. Which client protocols ship **enabled by default** — Thrift session only, or also REST and MQTT? → §2/§6.
5. Is the **inter-node** (ConfigNode↔DataNode + consensus) channel assumed to run on a trusted network, or is it authenticated/encrypted against an active network attacker? Proposed: trusted-network assumption unless TLS/mutual-auth is configured. → §4/§7.
6. Is **TLS** available and is it on or off by default for (a) client and (b) inter-node traffic? → §5a/§9.

**Wave 3 — extension execution & adversary:**
7. **The big one:** UDFs / Triggers / Pipe processors / AINode models execute user-supplied code server-side. Is registration **admin-only**, or grantable to non-admin users via `USE_UDF`/etc.? And is server-side code execution by a sufficiently-privileged principal **by-design** (→ `BY-DESIGN`) rather than a vulnerability? Proposed: by-design, gated to a trusted privilege. → §9/§11a/§13.
8. **Cluster Byzantine posture:** does IoTDB claim any safety/liveness against an authenticated-but-malicious peer node, or is cluster membership assumed fully trusted? Proposed: membership trusted; no Byzantine-fault tolerance claimed. → §7/§8.

**Wave 4 — properties & resource line:**
9. **Resource/DoS line:** is a single expensive query or a write flood that exhausts CPU/memory/disk a **bug** or expected-and-operator-managed? Where is the line (super-linear? unbounded allocation? a hang?)? → §8/§11a.
10. Are there other recurring scanner/fuzzer false positives the PMC already knows about (to seed §11a)?
11. **Meta:** IoTDB has no in-repo `SECURITY.md` today and an `AGENTS.md` that is a developer/build guide. This engagement adds `SECURITY.md` + `THREAT_MODEL.md` and wires `AGENTS.md → SECURITY.md → THREAT_MODEL.md`. Confirm where the canonical model should live (in-repo, as proposed, vs. the project website) and who owns revisions.
