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

import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.ClientManager;
import org.apache.iotdb.cluster.client.ClientManager.Type;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.qp.physical.sys.DummyPlan;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ExprBench {

  private AtomicLong requestCounter = new AtomicLong();
  private AtomicLong latencySum = new AtomicLong();
  private long maxLatency = 0;
  private int threadNum = 64;
  private int workloadSize = 64 * 1024;
  private int printInterval = 1000;
  private ClientManager clientPool;
  private Node target;
  private int maxRequestNum;
  private ExecutorService pool = Executors.newCachedThreadPool();
  private List<Node> nodeList = new ArrayList<>();
  private int raftFactor = 1;

  public ExprBench(Node target) {
    this.target = target;
    clientPool = new ClientManager(false, Type.MetaGroupClient);
  }

  public void benchmark() {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < threadNum; i++) {
      int finalI = i;
      pool.submit(
          () -> {
            Random random = new Random(123456L + finalI);
            Client client = null;
            try {
              client = clientPool.borrowSyncClient(target, ClientCategory.META);
            } catch (IOException e) {
              e.printStackTrace();
            }
            ExecutNonQueryReq request = new ExecutNonQueryReq();
            DummyPlan plan = new DummyPlan();
            plan.setWorkload(new byte[workloadSize]);
            plan.setNeedForward(true);

            ByteBuffer byteBuffer = ByteBuffer.allocate(workloadSize + 4096);

            long currRequsetNum = -1;
            while (true) {

              if (raftFactor > 0) {
                Node node = nodeList.get(random.nextInt(nodeList.size()));
                int raftId = random.nextInt(raftFactor);
                plan.setGroupIdentifier(ClusterUtils.nodeToString(node) + "#" + raftId);
              }
              byteBuffer.clear();
              plan.serialize(byteBuffer);
              byteBuffer.flip();
              request.planBytes = byteBuffer;
              request.setPlanBytesIsSet(true);

              long reqLatency = System.nanoTime();
              try {
                client.executeNonQueryPlan(request);
                currRequsetNum = requestCounter.incrementAndGet();
                if (currRequsetNum > threadNum * 10) {
                  reqLatency = System.nanoTime() - reqLatency;
                  maxLatency = Math.max(maxLatency, reqLatency);
                  latencySum.addAndGet(reqLatency);
                }
              } catch (TException e) {
                e.printStackTrace();
              }

              if (currRequsetNum % printInterval == 0) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println(
                    String.format(
                        "%d %d %f(%f) %f %f",
                        elapsedTime,
                        currRequsetNum,
                        (currRequsetNum + 0.0) / elapsedTime,
                        currRequsetNum * workloadSize / (1024.0 * 1024.0) / elapsedTime,
                        maxLatency / 1000.0,
                        (latencySum.get() + 0.0) / currRequsetNum));
              }

              if (currRequsetNum >= maxRequestNum) {
                break;
              }
            }
          });
    }
    pool.shutdown();
  }

  public void setMaxRequestNum(int maxRequestNum) {
    this.maxRequestNum = maxRequestNum;
  }

  public static void main(String[] args) {
    ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(50000);
    Node target = new Node();
    target.setInternalIp(args[0]);
    target.setMetaPort(Integer.parseInt(args[1]));
    ExprBench bench = new ExprBench(target);
    bench.maxRequestNum = Integer.parseInt(args[2]);
    bench.threadNum = Integer.parseInt(args[3]);
    bench.workloadSize = Integer.parseInt(args[4]) * 1024;
    bench.printInterval = Integer.parseInt(args[5]);
    String[] nodesSplit = args[6].split(",");
    for (String s : nodesSplit) {
      String[] nodeSplit = s.split(":");
      Node node = new Node();
      node.setInternalIp(nodeSplit[0]);
      node.setMetaPort(Integer.parseInt(nodeSplit[1]));
      bench.nodeList.add(node);
    }
    bench.raftFactor = Integer.parseInt(args[7]);

    bench.benchmark();
  }
}
