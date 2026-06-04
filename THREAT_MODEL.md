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
- **Draft confidence:** ~13 documented / ~40 maintainer-confirmed mentions / ~36 inferred (tag occurrences, counting the §14 resolution records). This is a v0 that has had its first PMC pass: the deployment posture, default-credential disposition, default-enabled protocols (REST/MQTT off; client Thrift SSL off), extension-privilege model, and the resource/DoS line were confirmed by an IoTDB PMC member (HTHou) in PR review and are now *(maintainer)*; the remaining trust-boundary and adversary claims (inter-node posture, Byzantine peer, TsFile/SDK boundary, long-term triage-policy/canonical-location wording) stay hypotheses for the PMC to confirm or correct.
- **What IoTDB is:** Apache IoTDB is a time-series database management system for IoT data — collection, storage, query, and analysis. It runs as a server (standalone, or a distributed cluster of ConfigNodes for metadata/coordination and DataNodes for storage/query), stores data in the TsFile columnar format, and is accessed over a Thrift-based RPC protocol via a SQL-like language and JDBC/session clients. *(documented — README, repo `CLAUDE.md`)*

## §2 Scope and intended use

- **Primary use:** an operator-deployed time-series database server, written to by IoT/device data pipelines and read by analytics clients, over the network. IoTDB should generally be treated as operator-deployed infrastructure, deployed inside a **trusted network by default**, with the application/ingestion tier in front of it. *(maintainer — HTHou: "IoTDB should generally be treated as operator-deployed infrastructure … trusted-network-by-default")*
- **Deployment shapes:** standalone single-node, and a distributed cluster (ConfigNode + DataNode roles, inter-node consensus). *(documented — repo `CLAUDE.md`)*
- **Caller roles** (this is a network service, not an in-process library — the "caller" splits):
  - **client** — connects over the Thrift session protocol / JDBC / SQL; authenticates with a username/password and is constrained by RBAC privileges. Treated as **untrusted** beyond its granted privileges. The authenticated client RPC surface is the main in-model boundary. *(maintainer — HTHou: "the client RPC surface as the main in-model boundary")*
  - **operator/admin** — the `root` superuser and whoever controls the deployment, configs, and the host. **Trusted** for the instance. *(inferred)*
  - **peer node** — another ConfigNode/DataNode in the same cluster, authenticated into the cluster. Trusted to the extent the cluster security model trusts members. *(inferred)*

**Component-family table** *(in/out = in/out of this model; all rows inferred unless noted)*:

| Family | Entry point | Touches outside process | In model? |
| --- | --- | --- | --- |
| Client RPC / session + SQL/query engine | Thrift session protocol (`iotdb-protocol/thrift-datanode`), JDBC, SQL | network (listens), filesystem (TsFile) | **In** *(documented: Thrift modules exist; maintainer: main in-model boundary)* |
| Authentication / RBAC | login + privilege checks (Users/Roles/Privileges) | — | **In** *(documented)* |
| Cluster control + consensus | ConfigNode RPC, inter-node consensus (`thrift-confignode`, `thrift-consensus`) | network (inter-node) | **In** *(documented: modules exist; trust posture inferred)* |
| Extension / server-side execution | UDF (`USE_UDF`), Triggers (`USE_TRIGGER`), Pipe (`USE_PIPE`), Models/AINode (`USE_MODEL`), templates | runs user-supplied logic/JARs; Pipe opens network | **In, but see §9** *(documented: privileges exist; maintainer: grantable system privileges, RBAC is the boundary)* |
| REST API / MQTT ingestion | HTTP REST service, MQTT broker (if enabled) | network (listens) | **In if enabled; both disabled by default** *(maintainer — HTHou: `enable_rest_service=false`, `enable_mqtt_service=false`)* |
| TsFile on-disk format | `apache/tsfile` (separate repo) | filesystem | **Out** — separate repo, model separately *(documented: TsFile is a separate project)* |
| Client SDKs | `iotdb-client-{go,nodejs,csharp}` (separate repos) | — | **Out of this batch** — deferred to later submissions *(per engagement scope)* |
| `example/`, `integration-test/` | demo + test code | — | **Out** *(see §3)* |

## §3 Out of scope (explicit non-goals)

- **Attackers who already control the host or the IoTDB process / config files / data directory.** They have the operator's authority by definition. *(inferred)*
- **`example/`, `integration-test/`, build/distribution tooling.** Shipped in the repo but not a production trust surface; threat-model separately if ever promoted. *(inferred)*
- **TsFile format internals** — owned by `apache/tsfile`; a parsing/decoding finding in TsFile is routed there, not here (this model covers IoTDB's *use* of TsFile, not the format library). *(inferred — confirm the boundary)*
- **The client SDK repos** (`iotdb-client-go/nodejs/csharp`) — out of this scan batch by agreement; each is enrolled separately as its discoverability lands. *(documented — engagement scope)*
- **Direct public exposure of the server**, especially with default credentials, is **not a supported production posture** — a report that depends on it is not in-model as a project defect; it is a deployment misconfiguration (see §10/§11). *(maintainer — HTHou: "Direct public exposure, especially with default credentials, should not be considered a supported production posture.")*
- **Confidentiality of data at rest / in transit when the operator has not enabled encryption** — see §10; TLS/disk-encryption posture is the operator's deployment responsibility unless the project claims otherwise. *(inferred)*

## §4 Trust boundaries and data flow

- **Primary trust boundary: the authenticated client RPC surface.** This is the main in-model boundary. Bytes arriving over the Thrift session protocol (and any enabled REST/MQTT endpoint) from a client are untrusted; a client is constrained to its RBAC-granted privileges. The query engine, schema engine, and storage layer sit behind this boundary. *(maintainer — HTHou: "the client RPC surface as the main in-model boundary")*
- **Secondary boundary: the cluster/inter-node surface.** ConfigNode↔DataNode RPC and the consensus channel — whether this is assumed to run on a trusted private network, or is itself authenticated/encrypted against an active network attacker, is left as an explicit open question (see §14); the PMC's suggestion is to settle it through follow-up discussion rather than in this PR. *(inferred — HTHou flagged inter-node trust as an open question to settle later)*
- **Reachability preconditions per component** (the test a triager applies before anything else):
  - A finding in the query/SQL/schema engine is **in-model** only if reachable from a client operating *within its granted privileges* (or from an unauthenticated pre-login surface). *(inferred)*
  - A finding requiring an already-`root`/admin session is **out-of-model: trusted-input** unless it crosses into host compromise the operator didn't already have. *(inferred)*
  - A finding in UDF/Trigger/Pipe/Model execution is in-model only subject to the §9 ruling: these are grantable system privileges, so the question is whether the principal granted them is trusted for that server-side execution capability (RBAC is the boundary, not a sandbox). *(maintainer — HTHou: see §9)*
  - A finding on the inter-node channel is in-model only if the cluster threat posture (§14) treats that channel as exposed. *(inferred)*

## §5 Assumptions about the environment

- **Runtime:** JVM, Java 1.8+ (1.8–25 verified). *(documented — README)*
- **OS:** Windows / macOS / Linux. *(documented — README)*
- **Filesystem:** the data/WAL/TsFile directories are private to the IoTDB process and not writable by untrusted local users; `max open files` raised to 65535. *(documented for the fd limit; filesystem-privacy inferred)*
- **Concurrency:** the server is multi-threaded and serves concurrent client sessions; thread-safety of the storage/query path is a correctness assumption. *(inferred)*
- **Clock:** time-series semantics depend on timestamps; whether server-side time ordering assumes monotonic/synchronized clocks across the cluster is open. *(inferred)*
- **What the server does to its host** (negative inventory — predominantly inferred, a wave-1/2 confirmation target): listens on network ports; reads/writes its data + WAL directories; reads its config files; spawns no child processes *except* where features explicitly do (e.g. Pipe sinks, external scripts?); the UDF/Trigger/Model features load and execute user-supplied code in-process. *(inferred)*

## §5a Build-time and configuration variants

Knobs that change which security properties hold:

- **Authentication enabled / default `root:root`** — IoTDB ships a single fixed administrator `root` with password `root`. *(documented — Authority-Management docs)* This default exists for initial setup and local getting-started use; it is a **must-change before production use or exposure outside a trusted environment**, and is **not a supported production posture**. A report that merely observes the default credential is therefore `OUT-OF-MODEL: non-default-build` (operator must-change), not a code bug in itself. *(maintainer — HTHou: "the default administrator account/password exists for initial setup and local getting-started use … must-change before production use or exposure outside a trusted environment, not as a supported production posture")*
- **REST API — disabled by default** (`enable_rest_service=false`). In-model only when the operator has explicitly enabled it. *(maintainer — HTHou)*
- **MQTT — disabled by default** (`enable_mqtt_service=false`). In-model only when explicitly enabled. *(maintainer — HTHou)*
- **Client Thrift SSL — available but disabled by default** (`enable_thrift_ssl=false`). Transport confidentiality/integrity on the client channel is therefore off unless the operator turns it on; see §9/§10. *(maintainer — HTHou)*
- **Inter-node TLS / wire encryption** — default on or off for the cluster channel? *(inferred — see §14)*
- **UDF / Trigger / Pipe / AINode-model execution** — gated by grantable system privileges (`USE_UDF`/`USE_TRIGGER`/`USE_PIPE`/`USE_MODEL`); see §9. *(maintainer — HTHou)*
- **Whitelist / network bind** (bind address, client allow-list) defaults. *(inferred)*

## §6 Assumptions about inputs

Per-surface trust table *(inferred unless noted; REST/MQTT rows apply only when the operator has enabled those services — both are off by default per §5a)*:

| Surface | Input | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| Thrift session `openSession` / login | username, password | **yes** (pre-auth) | strong `root` password; lock-out / rate-limit posture |
| Thrift session execute (SQL / inserts) | SQL text, tablet/row payloads, paths | **yes**, within granted privileges | privilege grants; query resource limits |
| REST API (only if enabled; default off) | HTTP body, headers, auth | **yes** | TLS, auth, network exposure |
| MQTT (only if enabled; default off) | topic, payload | **yes** | auth, network exposure |
| UDF / Trigger / Pipe / Model registration | JAR / class / model artifact | **yes if the relevant grantable system privilege is held** | who may hold `USE_UDF`/`USE_TRIGGER`/`USE_PIPE`/`USE_MODEL` — those principals are trusted for server-side execution |
| Inter-node RPC / consensus | peer messages | **yes if the cluster network is exposed** | trusted network or mutual auth |
| Config files, JVM flags, data dir | local | no — operator-trusted | filesystem permissions |

- **Shape/rate:** whether the server bounds per-query CPU/memory, result-set size, or concurrent sessions — and the line between a bug and operator-managed capacity — is set in §8 (resource line) and confirmed by the PMC; see §8/§9. *(maintainer — HTHou: see resource line)*

## §7 Adversary model

- **Primary adversary:** a network client that can reach the IoTDB RPC port (or an enabled REST/MQTT port) **from within the trusted-network deployment posture** — either **unauthenticated** (pre-login) or **authenticated with limited privileges** — trying to read/write data outside its grants, escalate privilege, execute code on the server beyond its granted extension privileges, crash/exhaust the server via malformed/pre-auth input, or move laterally to peer nodes. *(maintainer for the posture — HTHou; the per-vector detail remains inferred)*
- **Capabilities assumed:** can open connections, send arbitrary protocol/SQL bytes, supply large/malformed payloads, and (if granted the relevant privilege) register extensions. *(inferred)*
- **Out of scope:** anyone with `root`/admin session or host/process/filesystem control (already authoritative); an attacker who only reaches the server because it was directly publicly exposed (non-supported posture, §3); side-channel/timing adversaries (unless the PMC wants them in). *(maintainer for public-exposure exclusion — HTHou; rest inferred)*
- **Cluster — authenticated-but-Byzantine peer:** a node that holds a valid cluster identity and then behaves arbitrarily. Whether IoTDB's consensus claims safety/liveness against such a peer (and under what honest-fraction threshold), or whether cluster membership is assumed fully trusted, is left as an explicit open question for follow-up PMC discussion (see §14). *(inferred — HTHou: keep Byzantine-peer assumptions as an open question)*

## §8 Security properties the project provides

*(Inferred pending PMC confirmation unless tagged — a property only counts once the project commits to it.)*

- **Authentication + RBAC enforcement.** A client cannot read/write series data or schema, or perform global operations, beyond the privileges granted to its user/roles; unauthenticated clients cannot act. *Violation symptom:* an unprivileged session reads/writes data or runs admin operations. *Severity:* security-critical (auth bypass / privilege escalation). *(inferred — the RBAC model is documented; the guarantee that it is unbypassable is the claim to confirm)*
- **Memory/availability safety on the pre-auth + client RPC surface.** Malformed or pre-auth client input (Thrift/SQL/REST) yields a clean error, not a crash, OOM, or hang of the server. *Violation symptom:* server crash / unbounded allocation / deadlock from malformed or pre-auth client input. *Severity:* security-critical (remote DoS) if pre-auth; lower if it requires privilege. This is the in-model half of the resource/DoS line (see §9 for what is out). *(maintainer — HTHou: malformed/pre-auth/client input causing crashes, OOMs, deadlocks, or clearly unbounded behavior stays in-model security-relevant behavior)*
- **Tenant/path isolation.** A client with privilege on one path/database cannot read or write series outside its granted paths. *Violation symptom:* cross-path data access. *Severity:* security-critical. *(inferred)*
- **Resource bounds — split, not unspecified.** Malformed/pre-auth input that crashes/OOMs/hangs the server is **in-model** (above). Ordinary expensive queries or write load are **operator capacity/resource-management**, NOT in-model — *unless* there is a specific bug: super-linear amplification, a missing limit where a limit is expected, or a hang. See §9 and §11a. *(maintainer — HTHou)*

## §9 Security properties the project does *not* provide

*(The highest-value section for integrators — inferred unless tagged, confirm each.)*

- **Server-side code execution via extensions is by-design, not a vulnerability — gated by RBAC.** UDFs, Triggers, Pipe processors, and AINode models execute user-supplied logic/JARs **inside the server process**. `USE_UDF`, `USE_TRIGGER`, `USE_PIPE`, and `USE_MODEL` are **grantable system privileges** (not strictly root/admin-only). The security-model interpretation is that **principals granted these privileges are trusted for the corresponding server-side execution capability**; RBAC is the authorization boundary here, **not a sandbox**. A scan reporting "UDF/Trigger/Pipe/Model allows arbitrary code execution" is therefore `BY-DESIGN` for a principal holding the relevant grant. *(maintainer — HTHou: "system privileges and … grantable privileges, not strictly root/admin-only … principals granted these privileges are trusted for the corresponding server-side execution capability. RBAC is the authorization boundary here, not a sandbox.")*
- **Transport confidentiality/integrity is the operator's job** unless TLS is enabled — client Thrift SSL is off by default (`enable_thrift_ssl=false`, §5a), so by default IoTDB does not defend against a network attacker reading/modifying client traffic. The inter-node channel's default posture is an open question (§14). *(maintainer for the client-SSL default — HTHou; inter-node inferred)*
- **No defense against a malicious operator / `root`.** *(inferred)*
- **Default-credential exposure is not IoTDB's bug** — `root:root` is a must-change-before-production default, not a supported posture (§5a/§10). *(maintainer — HTHou)*
- **Ordinary resource exhaustion is not a defended property.** Ordinary expensive queries or write load that consume CPU/memory/disk are an operator capacity/resource-management concern, not an in-model security property — unless a specific bug applies (super-linear amplification, a missing limit where a limit is expected, or a hang), in which case it is in-model (§8). *(maintainer — HTHou)*
- **False friends:** RBAC privileges are an *authorization* mechanism, not a *sandbox* — granting `USE_UDF`/`USE_TRIGGER`/`USE_PIPE`/`USE_MODEL` is a grant of server-side code execution, not a safe boundary against hostile code. The username/password login is not a defense against a network MITM without TLS (off by default). *(maintainer — HTHou: "RBAC is the authorization boundary here, not a sandbox")*
- **Well-known classes left to the caller:** SQL-injection-style abuse via clients that build IoTDB SQL from their *own* untrusted input (the embedding app's responsibility); ordinary resource-exhaustion via expensive queries (above); credential-stuffing against the login surface. *(inferred)*

## §10 Downstream responsibilities (operator/deployer)

*(Inferred unless tagged — confirm.)*

- Change the default `root` password before any production use or exposure outside a trusted environment. *(maintainer — HTHou: must-change before production)*
- Deploy IoTDB inside a trusted network boundary; do **not** expose the client RPC port (or an enabled REST/MQTT port) directly to an untrusted/public network, especially with default credentials — that is not a supported posture. Enable TLS + auth if a wider exposure is unavoidable. *(maintainer — HTHou: trusted-network-by-default; public exposure with default creds not supported)*
- Run the inter-node cluster channel on a trusted/segmented network (or enable mutual auth/TLS if supported). *(inferred — pending the §14 inter-node ruling)*
- Restrict who is granted the extension system privileges (`USE_UDF`/`USE_TRIGGER`/`USE_PIPE`/`USE_MODEL`) — holding one is equivalent to server-side code execution; RBAC is the boundary, not a sandbox. *(maintainer — HTHou)*
- Enable client Thrift SSL (`enable_thrift_ssl=true`) where client traffic crosses an untrusted segment — it is off by default. *(maintainer — HTHou)*
- Set filesystem permissions so only the IoTDB user can read the data/WAL/config directories. *(inferred)*
- Apply per-query / per-session resource limits appropriate to capacity — ordinary expensive-query/write-load DoS is an operator capacity concern (§8/§9). *(maintainer — HTHou)*

## §11 Known misuse patterns

*(Draft one-liners — expand before publishing.)*

- Exposing IoTDB directly to the public internet, especially with default `root:root` credentials (non-supported posture, §3). *(maintainer — HTHou)*
- Granting extension system privileges (`USE_UDF`/etc.) to semi-trusted clients, treating RBAC as a sandbox rather than an authorization boundary. *(maintainer — HTHou)*
- Building IoTDB SQL by string-concatenating the embedding application's untrusted input. *(inferred)*
- Running a cluster across an untrusted network without inter-node protection. *(inferred)*

## §11a Known non-findings (recurring false positives)

*(Inferred unless tagged; the PMC's confirmations here are the highest-leverage suppression input for the scan.)*

- "UDF/Trigger/Pipe/Model can run arbitrary code" — by-design server-side execution gated by the grantable `USE_UDF`/`USE_TRIGGER`/`USE_PIPE`/`USE_MODEL` system privileges (§9); the principal holding the grant is trusted for that capability. Not a finding. *(maintainer — HTHou)*
- "Default password is `root`" — operator must-change-before-production per §5a/§10; `OUT-OF-MODEL: non-default-build`, not a code bug in itself. *(maintainer — HTHou)*
- "Server reachable on the public internet / no TLS by default on the wire" — operator deployment responsibility (§9/§10); client Thrift SSL is off by default and public exposure is a non-supported posture (§3). *(maintainer — HTHou)*
- "Admin/`root` can do destructive operation X" — out-of-model: the admin is trusted (§7). *(inferred)*
- "Expensive query / write load consumes lots of CPU/memory/disk" — operator capacity concern, NOT a finding, unless a specific bug applies: super-linear amplification, a missing limit where a limit is expected, or a hang (§8/§9). *(maintainer — HTHou)*

## §12 Conditions that would change this model

- A new client-facing protocol or endpoint; a change to the default auth posture (e.g. `root:root` → forced change), default TLS, or default-enabling of REST/MQTT (both off today). *(maintainer for current defaults — HTHou)*
- Promoting a client SDK or `example/` code into the production trust surface. *(inferred)*
- A change to the cluster trust/consensus posture. *(inferred)*
- **A report that cannot be routed to a single §13 disposition** — signals a model gap; revise the model rather than make an ad-hoc call.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope adversary/input (auth bypass, cross-path access, pre-auth/malformed-input crash/OOM/hang, privilege escalation). | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but the API/SQL makes a §11 misuse easy enough to harden. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires an admin/`root` session or operator-controlled config/files. | §6, §7 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires a capability the model excludes (host control, side channel, direct public exposure, exposed inter-node net when posture says trusted). | §3, §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `example/`, `integration-test/`, or the separate TsFile / SDK repos. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests under a discouraged/non-default §5a setting (e.g. unchanged `root:root`, or an explicitly-enabled REST/MQTT used as if default). | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (extension code execution within its RBAC grant, no-TLS-by-default, ordinary resource exhaustion, malicious operator). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `MODEL-GAP` | Cannot be cleanly routed — triggers a §12 revision. | §12 |

## §14 Open questions for the maintainers

Several wave-1 items below were **confirmed by an IoTDB PMC member (HTHou) in PR review** and folded into the body; they are retained here only as a record of the resolution. The genuinely-open items are the inter-node trust posture, the Byzantine-peer assumption, and the long-term triage-policy/canonical-location wording — which HTHou suggested settling through follow-up PMC discussion rather than finalizing in this PR.

**Wave 1 — scope & intended posture** (resolved by HTHou; recorded):
1. **Deployment posture:** *resolved* — trusted-network-by-default, operator-deployed infrastructure, client RPC surface as the main in-model boundary; direct public exposure (esp. with default creds) is not a supported posture. → §2/§3/§4/§7. *(maintainer)*
2. **Default `root:root`:** *resolved* — must-change before production/exposure, not a supported production posture; report = `OUT-OF-MODEL: non-default-build`. → §5a/§10/§11a. *(maintainer)*
3. **TsFile boundary + SDK repos:** still open — confirm TsFile findings route to `apache/tsfile`, and the `iotdb-client-*` SDKs are out of this batch. → §2/§3. *(inferred)*

**Wave 2 — trust boundaries & protocols:**
4. **Default-enabled protocols:** *resolved* — Thrift session is the primary surface; REST (`enable_rest_service=false`) and MQTT (`enable_mqtt_service=false`) are **disabled by default**. → §2/§5a/§6. *(maintainer)*
5. **Inter-node trust (open):** is the ConfigNode↔DataNode + consensus channel assumed to run on a trusted network, or authenticated/encrypted against an active network attacker? HTHou suggested settling this via follow-up PMC discussion. Proposed interim: trusted-network assumption unless TLS/mutual-auth is configured. → §4/§7. *(inferred)*
6. **TLS defaults:** *partially resolved* — client Thrift SSL is **off by default** (`enable_thrift_ssl=false`). Still open: is TLS available/default for the **inter-node** channel? → §5a/§9. *(client part maintainer; inter-node part inferred)*

**Wave 3 — extension execution & adversary:**
7. **Extension privilege model:** *resolved* — `USE_UDF`/`USE_TRIGGER`/`USE_PIPE`/`USE_MODEL` are grantable system privileges (not strictly root-only); server-side code execution by a principal holding the grant is **by-design**; RBAC is the boundary, not a sandbox. → §9/§11a/§13. *(maintainer)*
8. **Cluster Byzantine posture (open):** does IoTDB claim any safety/liveness against an authenticated-but-malicious peer node, or is cluster membership assumed fully trusted? HTHou suggested keeping this as an explicit open question for follow-up. Proposed interim: membership trusted; no Byzantine-fault tolerance claimed. → §7/§8. *(inferred)*

**Wave 4 — properties & resource line:**
9. **Resource/DoS line:** *resolved* — malformed/pre-auth/client input causing crash/OOM/deadlock/clearly-unbounded behavior is in-model; ordinary expensive queries or write load are an operator capacity concern unless a specific bug applies (super-linear amplification, missing-expected-limit, or a hang). → §8/§9/§11a. *(maintainer)*
10. Are there other recurring scanner/fuzzer false positives the PMC already knows about (to seed §11a)? → §11a. *(inferred)*
11. **Meta / long-term triage policy (open):** IoTDB has no in-repo `SECURITY.md` today and an `AGENTS.md` that is a developer/build guide. This engagement adds `SECURITY.md` + `THREAT_MODEL.md` and wires `AGENTS.md → SECURITY.md → THREAT_MODEL.md`. Confirm where the canonical model should live (in-repo, as proposed, vs. the project website), who owns revisions, and the exact wording of the long-term triage policy — which HTHou suggested settling through follow-up PMC discussion. → §1. *(inferred)*
