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

package org.apache.iotdb.cluster.expr;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.manage.MetaSingleSnapshotLogManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.qp.physical.sys.DummyPlan;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SequencerExpr extends MetaGroupMember {

  private int threadNum = 1000;
  private AtomicLong reqCnt = new AtomicLong();

  public SequencerExpr() {
    LogApplier applier =
        new LogApplier() {
          @Override
          public void apply(Log log) {
            log.setApplied(true);
          }

          @Override
          public void close() {}
        };
    logManager = new MetaSingleSnapshotLogManager(applier, this);

    reportThread = Executors.newSingleThreadScheduledExecutor();
    reportThread.scheduleAtFixedRate(
        this::generateNodeReport, REPORT_INTERVAL_SEC, REPORT_INTERVAL_SEC, TimeUnit.SECONDS);
    logSequencer = SEQUENCER_FACTORY.create(this, logManager);
  }

  @Override
  public Client getSyncClient(Node node) {
    return new Client(null, null) {
      @Override
      public AppendEntryResult appendEntry(AppendEntryRequest request) {
        return new AppendEntryResult().setStatus(Response.RESPONSE_STRONG_ACCEPT);
      }

      @Override
      public AppendEntryResult appendEntries(AppendEntriesRequest request) {
        return new AppendEntryResult().setStatus(Response.RESPONSE_STRONG_ACCEPT);
      }
    };
  }

  private void sequencing() {
    for (int i = 0; i < threadNum; i++) {
      new Thread(
              () -> {
                while (true) {
                  reqCnt.incrementAndGet();
                  DummyPlan dummyPlan = new DummyPlan();
                  processPlanLocallyV2(dummyPlan);
                }
              })
          .start();
    }
  }

  private void startMonitor() {
    new Thread(
            () -> {
              long startTime = System.currentTimeMillis();
              while (true) {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                long consumedTime = System.currentTimeMillis() - startTime;
                System.out.println(
                    "" + consumedTime + ", " + (reqCnt.get() * 1.0 / consumedTime * 1000L));
              }
            })
        .start();
  }

  public static void main(String[] args) {
    RaftMember.USE_LOG_DISPATCHER = true;
    ClusterDescriptor.getInstance().getConfig().setEnableRaftLogPersistence(false);
    SequencerExpr sequencerExpr = new SequencerExpr();
    sequencerExpr.setCharacter(NodeCharacter.LEADER);
    PartitionGroup group = new PartitionGroup();
    for (int i = 0; i < 3; i++) {
      group.add(new Node().setNodeIdentifier(i).setMetaPort(i));
    }
    sequencerExpr.setAllNodes(group);
    sequencerExpr.sequencing();
    sequencerExpr.startMonitor();
  }
}
