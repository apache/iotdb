/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;

/** LogRelay is used by followers to forward entries from the leader to other followers. */
public class LogRelay {

  private static final Logger logger = LoggerFactory.getLogger(LogRelay.class);

  private ConcurrentSkipListSet<RelayEntry> entryHeap = new ConcurrentSkipListSet<>();
  private static final int RELAY_NUMBER =
      ClusterDescriptor.getInstance().getConfig().getRelaySenderNum();
  private ExecutorService relaySenders;
  private RaftMember raftMember;

  public LogRelay(RaftMember raftMember) {
    this.raftMember = raftMember;
    relaySenders =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            RELAY_NUMBER, raftMember.getName() + "-RelaySender");
    for (int i = 0; i < RELAY_NUMBER; i++) {
      relaySenders.submit(new RelayThread());
    }
  }

  public void stop() {
    relaySenders.shutdownNow();
  }

  public void offer(AppendEntryRequest request, List<Node> receivers) {
    offer(new RelayEntry(request, receivers));
  }

  private void offer(RelayEntry entry) {
    long operationStartTime = Statistic.RAFT_SENDER_RELAY_OFFER_LOG.getOperationStartTime();
    synchronized (entryHeap) {
      while (entryHeap.size()
          > ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem()) {
        try {
          entryHeap.wait(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      entryHeap.add(entry);
      entryHeap.notifyAll();
    }
    Statistic.RAFT_SENDER_RELAY_OFFER_LOG.calOperationCostTimeFromStart(operationStartTime);
  }

  public void offer(AppendEntriesRequest request, List<Node> receivers) {
    offer(new RelayEntry(request, receivers));
  }

  private class RelayThread implements Runnable {

    @Override
    public void run() {
      String baseName = Thread.currentThread().getName();
      while (!Thread.interrupted()) {
        RelayEntry relayEntry;
        synchronized (entryHeap) {
          relayEntry = entryHeap.pollFirst();
          if (relayEntry == null) {
            try {
              entryHeap.wait(10);
              continue;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }

        logger.debug("Relaying {}", relayEntry);

        if (relayEntry.singleRequest != null) {
          Thread.currentThread()
              .setName(
                  baseName
                      + "-"
                      + (relayEntry.singleRequest.prevLogIndex + 1)
                      + "-"
                      + relayEntry.receivers);
          raftMember.sendLogToSubFollowers(relayEntry.singleRequest, relayEntry.receivers);
        } else if (relayEntry.batchRequest != null) {
          Thread.currentThread()
              .setName(
                  baseName
                      + "-"
                      + (relayEntry.batchRequest.prevLogIndex + 1)
                      + "-"
                      + relayEntry.receivers);
          raftMember.sendLogsToSubFollowers(relayEntry.batchRequest, relayEntry.receivers);
        }

        Statistic.RAFT_SEND_RELAY.add(1);
      }
    }
  }

  public static class RelayEntry implements Comparable<RelayEntry> {

    private AppendEntryRequest singleRequest;
    private AppendEntriesRequest batchRequest;
    private List<Node> receivers;

    public RelayEntry(AppendEntryRequest request, List<Node> receivers) {
      this.singleRequest = request;
      this.receivers = receivers;
    }

    public RelayEntry(AppendEntriesRequest request, List<Node> receivers) {
      this.batchRequest = request;
      this.receivers = receivers;
    }

    public long getIndex() {
      if (singleRequest != null) {
        return singleRequest.prevLogIndex;
      } else if (batchRequest != null) {
        return batchRequest.prevLogIndex;
      }
      return 0;
    }

    @Override
    public String toString() {
      long index = singleRequest != null ? singleRequest.prevLogIndex : batchRequest.prevLogIndex;
      index++;
      return "RelayEntry{" + index + "," + receivers + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RelayEntry that = (RelayEntry) o;
      return Objects.equals(singleRequest, that.singleRequest)
          && Objects.equals(batchRequest, that.batchRequest);
    }

    @Override
    public int hashCode() {
      return Objects.hash(singleRequest, batchRequest);
    }

    @Override
    public int compareTo(RelayEntry o) {
      return Long.compare(this.getIndex(), o.getIndex());
    }
  }

  public RelayEntry first() {
    try {
      return entryHeap.isEmpty() ? null : entryHeap.first();
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}
