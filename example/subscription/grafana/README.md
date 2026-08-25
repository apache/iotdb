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

# Consensus subscription Grafana dashboard

This directory contains a standalone Grafana dashboard and example Prometheus alert rules for
IoTConsensus-based subscriptions. The dashboard uses only metrics exposed by Apache IoTDB and does
not contain environment-specific datasource UIDs, hosts, ports, or credentials.

## Scope and limitations

The dashboard answers these operational questions with the metrics currently available:

- Is a subscription queue active and initialized for every DataRegion?
- Is the queue caught up after a stopped workload?
- Is work waiting in memory or behind the WAL cursor?
- Are consumers receiving bytes?
- Did WAL retention make search indexes unavailable?
- Did a seek or routing-epoch change occur?

The dashboard does **not** provide an exact remaining event count, a reliable ETA, or a
subscription-progress CLI. In a continuously written incremental topic there is no final
completion time. The subscription_consensus_lag
metric is an approximate queue-local work indicator: it includes queued, in-flight, pending,
realtime-buffered, and lingered entries, but all unread WAL work adds
only one unit regardless of how many WAL entries remain. The watermark is the maximum observed data
timestamp, not committed progress.

The potential-no-delivery signal means that an active, initialized queue has approximate work but
its cumulative response-byte counter has not increased for the alert window. It can identify a
stopped or non-polling consumer, but it cannot prove that ACK/commit progress is stalled because no
committed-progress timestamp or counter is currently exported.

## Prerequisites

Enable the DataNode Prometheus reporter at IMPORTANT level in iotdb-system.properties and restart
the DataNode:

~~~properties
dn_metric_reporter_list=PROMETHEUS
dn_metric_level=IMPORTANT
dn_metric_prometheus_reporter_port=9092
~~~

If subscription has been disabled explicitly, also set subscription_enabled=true.

Configure Prometheus to scrape the /metrics endpoint of every DataNode. For example:

~~~yaml
scrape_configs:
  - job_name: iotdb-datanode
    metrics_path: /metrics
    static_configs:
      - targets:
          - datanode-1:9092
          - datanode-2:9092
          - datanode-3:9092
~~~

Consensus subscription series exist only while a record-handler incremental subscription queue is
registered. Create and poll a subscription before treating an empty query as a scrape failure. The
legacy topic value mode=consensus is normalized to mode=incremental.

Verify a DataNode directly before importing the dashboard:

~~~shell
curl http://datanode-1:9092/metrics | grep -E '^subscription_(consensus|event|uncommitted)'
~~~

At minimum, a running consensus subscription should expose subscription_consensus_lag,
subscription_consensus_active, subscription_consensus_initialized, and the other metrics listed
below. Prometheus must also add its normal instance label during scraping.

## Import

1. In Grafana, select **Dashboards > New > Import**.
2. Upload consensus-subscription-dashboard.json.
3. Select the Prometheus datasource requested by DS_PROMETHEUS.
4. Select cluster, instance, queue, and DataRegion variables.

The queue label is currently exported as name=<consumerGroupId>_<topicName>. It identifies a
consumer-group/topic queue, not an individual consumer, and the underscore encoding can be
ambiguous when both names contain underscores.

## Metric semantics

| Metric | Dashboard interpretation |
| --- | --- |
| subscription_consensus_lag | Approximate work, not an exact remaining count |
| subscription_uncommitted_event_count | Delivered in-flight events awaiting commit |
| subscription_event_transfer_total (no `rate` label) | Cumulative response bytes |
| subscription_event_transfer_total{rate="m1"} | One-minute response transfer rate in bytes/second |
| subscription_consensus_wal_gap | Cumulative unavailable WAL search indexes skipped |
| subscription_consensus_routing_epoch_change | Cumulative routing epoch changes |
| subscription_consensus_watermark | Maximum observed data timestamp in the server timestamp precision |
| subscription_consensus_seek_generation | Seek/reset generation; it is not a commit ID |
| subscription_consensus_active | 1 on the preferred-writer queue and 0 on inactive replicas |
| subscription_consensus_initialized | 1 after the queue runtime is initialized and 0 while dormant |

For a stopped workload, a region is heuristically caught up when its preferred-writer queue is
active, initialized, and has lag equal to zero. Completion of a bounded workload requires every
participating region to satisfy that condition. This heuristic must not be presented as completion
or ETA for an unbounded incremental stream.

## Example alerts

consensus-subscription-alert-rules.yml contains Prometheus rule examples for:

- newly skipped WAL indexes;
- no active preferred-writer queue for a group/topic/region;
- an active queue that remains dormant while work is pending;
- pending work with no response bytes transferred for ten minutes.

The last alert is deliberately named NoDelivery rather than StalledCommit. Tune its duration to the
consumer poll interval and expected workload. Add a deployment-specific absent-series alert only
when an external inventory says that a subscription must exist; absent series alone can also mean
that no consensus subscription is configured.

Validate the rules before loading them:

~~~shell
promtool check rules consensus-subscription-alert-rules.yml
~~~

## Suggested live validation

1. Start all DataNodes and confirm Prometheus up is 1 for their metric endpoints.
2. Run either ConsensusSubscriptionSessionExample or
   ConsensusTableModelSubscriptionSessionExample.
3. Confirm every expected queue/region has series and exactly one active replica.
4. Write a finite batch, keep polling and committing, and confirm the active initialized queues
   return to approximate lag zero after writes stop.
5. Stop the consumer, write more data, and confirm pending work remains while the cumulative
   response-byte counter stops increasing and the NoDelivery alert eventually fires.
6. Restart or transfer the region leader and confirm a new active replica appears without duplicate
   active queues.
7. Confirm WAL gap remains zero in normal operation. Any increase is a data-loss diagnostic and
   should be investigated immediately.
