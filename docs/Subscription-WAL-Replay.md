<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Consensus subscription WAL replay

Realtime requests can be rejected by the shared Consensus request memory pool or by
queue admission. The queue then needs WAL replay to fill the missing local search
indexes before delivering later realtime requests. `WAL replay required` describes
this requirement; it does not promise that WAL is already readable or that replay
will finish within a fixed time. `lastAdmissionRejectionCode` records the most
recent rejection and can remain set after the pressure has passed.

## Replay progress and latency

WAL scanning uses a shared per-round physical-entry budget and a cooperative
50 ms time slice. The time slice starts at the first WAL scan operation, after
queue housekeeping. A new iterator may skip historical files only when every
writer's progress in those files is already covered. An equal local search-index
lower bound still permits this realignment; the lower bound alone does not locate
the reader. Uncovered replicated requests remain replayable even when they have
no local search index. Reader position and incomplete request fragments survive
a yielded scan.

The retry delays for a WAL visibility gap and a memory block are 10 ms and 100 ms,
respectively. These are scheduling delays, not end-to-end recovery deadlines.
WAL visibility, retained history, disk throughput, worker contention, entry size,
and consumer ACK latency all affect recovery. A single I/O operation can exceed
the cooperative time slice. There is no finite recovery-time guarantee while a
required WAL file remains unreadable or memory remains unavailable. Do not skip
that file or advance the cursor to satisfy a latency target.

For a workload whose normal per-region delivery interval is below 5 seconds,
validate recovery against that same target under sustained writes and temporary
memory pressure. Treat 60 seconds without delivery while lag is positive as an
investigation threshold, not an acceptable recovery time. A cursor that makes no
progress for 60 seconds while lag remains positive also produces a server WARN,
at most once every 30 seconds until cursor progress resumes. The WARN and periodic
queue statistics include the last completed prefetch duration/result and WAL
reader state: scan yield, temporary unreadability, buffered request, or no
buffered request, together with file version, entry offset, and cumulative body
read attempts for that iterator. `SCAN_YIELDED` with advancing file/entry offsets
and increasing body reads identifies historical scanning despite a frozen
delivery cursor. Read retries can also increase the attempt counter, so interpret
it together with the reader state and position.

`prefetchDurationMs` measures the currently running round. Periodic statistics
are emitted near the start of that round, so a value of zero is expected there.
Use `lastPrefetchDurationMs` for the last completed round and `walReplayState` to
interpret replay activity. The reader state describes its latest WAL probe; a
round blocked before probing WAL can retain that state. Correlate consecutive
samples with `memoryBlockedEntryBytes`, `realtimeAdmissionBlocked`, and reader
position changes. `NO_BUFFERED_REQUEST` alone does not imply an empty WAL backlog.

## Monitoring each queue

Evaluate delivery and lag for each `(name, region)` queue on each DataNode.
Consumer-group totals and consumer-level poll intervals can hide a stalled region
when other regions keep delivering. An example Prometheus alert expression is:

```promql
(subscription_consensus_delivery_idle_time_ms >= 60000)
and (subscription_consensus_lag > 0)
and (subscription_consensus_active == 1)
and (subscription_consensus_initialized == 1)
```

These gauges use the same labels, so the expression matches the full queue and
DataNode label set. Consider a short `for` interval appropriate to the scrape
period. Capture prefetch worker stacks and a wall-clock profile when the alert
fires, along with queue statistics and WAL I/O information. The server warning
requires the prefetch worker to run; the delivery-idle gauge can also detect a
worker blocked inside I/O or waiting for a lock.

## Capacity under sustained writes

There are separate budgets for retained realtime requests in Consensus memory and
materialized subscription Tablets. Free subscription memory does not prove that
Consensus request admission succeeded, and a historical rejection does not prove
that materialization is currently blocked.

For a subscription materialization budget `M` and `N` active queues, each queue's
quota is approximately `M / N`. Half of that quota is protected from borrowing by
other queues. One queue's theoretical maximum is approximately
`M - (N - 1) * M / (2 * N)`, but actual availability falls as other queues retain
data. Size writes and consumer ACK cadence for the per-queue quota under concurrent
load, rather than relying on the theoretical borrowing maximum. With a budget of
987,063,313 bytes and ten active queues, the quota is about 94.1 MiB and the
protected share is about 47.1 MiB per queue.

Reduce write batch sizes or large fields when individual materialized requests
consume much of the quota. Size subscription memory for peak concurrent retained
Tablets and ACK latency, and leave headroom in the shared Consensus pool for the
write path and retained realtime requests. More consumers in one group do not
increase a single region's WAL scan concurrency. Validate sustained WAL replay
throughput above that region's write rate, retention sufficient for the measured
catch-up time, and recovery after rejected realtime requests before accepting a
capacity configuration.
