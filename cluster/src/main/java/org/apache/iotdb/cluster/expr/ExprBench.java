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

import org.apache.iotdb.cluster.client.sync.SyncClientFactory;
import org.apache.iotdb.cluster.client.sync.SyncClientPool;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient.FactorySync;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.db.qp.physical.sys.DummyPlan;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class ExprBench {

  private AtomicLong requestCounter = new AtomicLong();
  private int threadNum = 64;
  private int workloadSize = 64 * 1024;
  private SyncClientPool clientPool;
  private Node target;
  private int maxRequestNum;

  public ExprBench(Node target) {
    this.target = target;
    SyncClientFactory factory = new FactorySync(new Factory());
    clientPool = new SyncClientPool(factory);
  }

  public void benchmark() {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < threadNum; i++) {
      new Thread(
              () -> {
                Client client = clientPool.getClient(target);
                ExecutNonQueryReq request = new ExecutNonQueryReq();
                DummyPlan plan = new DummyPlan();
                plan.setWorkload(new byte[workloadSize]);
                plan.setNeedForward(true);
                ByteBuffer byteBuffer = ByteBuffer.allocate(workloadSize + 4096);
                plan.serialize(byteBuffer);
                byteBuffer.flip();
                request.setPlanBytes(byteBuffer);
                long currRequsetNum = -1;
                while (true) {

                  try {
                    client.executeNonQueryPlan(request);
                    currRequsetNum = requestCounter.incrementAndGet();
                  } catch (TException e) {
                    e.printStackTrace();
                  }

                  if (currRequsetNum % 1000 == 0) {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    System.out.println(
                        String.format(
                            "%d %d %f(%f)",
                            elapsedTime,
                            currRequsetNum,
                            (currRequsetNum + 0.0) / elapsedTime,
                            currRequsetNum * workloadSize / (1024.0 * 1024.0) / elapsedTime));
                  }

                  if (currRequsetNum >= maxRequestNum) {
                    break;
                  }
                }
              })
          .start();
    }
  }

  public void setMaxRequestNum(int maxRequestNum) {
    this.maxRequestNum = maxRequestNum;
  }

  public static void main(String[] args) {
    Node target = new Node();
    target.setInternalIp(args[0]);
    target.setMetaPort(Integer.parseInt(args[1]));
    ExprBench bench = new ExprBench(target);
    bench.maxRequestNum = Integer.parseInt(args[2]);
    bench.threadNum = Integer.parseInt(args[3]);
    bench.workloadSize = Integer.parseInt(args[4]) * 1024;
    bench.benchmark();
  }
}
