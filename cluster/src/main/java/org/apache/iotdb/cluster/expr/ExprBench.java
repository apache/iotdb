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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.qp.physical.sys.DummyPlan;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ExprBench {

  private static final Logger logger = LoggerFactory.getLogger(ExprBench.class);

  private AtomicLong requestCounter = new AtomicLong();
  private AtomicLong latencySum = new AtomicLong();
  private AtomicLong burstRequestCounter = new AtomicLong();
  private AtomicLong burstLatencySum = new AtomicLong();
  private long maxLatency = 0;
  private int threadNum = 64;
  private int workloadSize = 64 * 1024;
  private int printInterval = 1000;
  private ClientManager clientPool;
  private long maxRunningSecond;
  private long burstInterval;
  private int maxRequestNum;
  private ExecutorService pool = Executors.newCachedThreadPool();
  private List<Node> nodeList = new ArrayList<>();
  private int[] raftFactors;
  private int[] rateLimits;
  private List<EndPoint> endPoints = new ArrayList<>();
  private Map<EndPoint, RateLimiter> rateLimiterMap = new ConcurrentHashMap<>();
  private Map<EndPoint, Statistic> latencyMap = new ConcurrentHashMap<>();
  private long startTime;
  private volatile boolean duringBurst = false;
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public ExprBench(Node target) {
    clientPool = new ClientManager(false, Type.MetaGroupClient);
  }

  private static class EndPoint {

    private Node node;
    private int raftId;

    public EndPoint(Node node, int raftId) {
      this.node = node;
      this.raftId = raftId;
    }

    @Override
    public String toString() {
      return "EndPoint{" + "node=" + node.getInternalIp() + ", raftId=" + raftId + '}';
    }
  }

  private static class Statistic {

    private AtomicLong sum = new AtomicLong();
    private AtomicLong cnt = new AtomicLong();

    public void add(long val) {
      sum.addAndGet(val);
      cnt.incrementAndGet();
    }

    @Override
    public String toString() {
      return "{" + sum.get() + "," + cnt.get() + "," + (sum.get() * 1.0 / cnt.get()) + "}";
    }
  }

  private void benchmarkTask(int taskId) {
    int endPointIdx = taskId % endPoints.size();
    Client client = null;

    ExecutNonQueryReq request = new ExecutNonQueryReq();
    DummyPlan plan = new DummyPlan();
    plan.setWorkload(new byte[workloadSize]);
    plan.setNeedForward(true);

    ByteBuffer byteBuffer = ByteBuffer.allocate(workloadSize + 4096);
    Map<EndPoint, Node> endPointLeaderMap = new HashMap<>();

    Node target = null;
    long currRequsetNum = -1;
    while (true) {
      EndPoint endPoint = endPoints.get(endPointIdx);
      RateLimiter rateLimiter = rateLimiterMap.get(endPoint);
      if (rateLimiter != null) {
        rateLimiter.acquire(1);
      }

      target = endPointLeaderMap.getOrDefault(endPoint, endPoint.node);
      int raftId = endPoint.raftId;
      plan.setGroupIdentifier(ClusterUtils.nodeToString(endPoint.node) + "#" + raftId);

      try {
        client = clientPool.borrowSyncClient(target, ClientCategory.META);
      } catch (IOException e) {
        e.printStackTrace();
      }

      byteBuffer.clear();
      plan.serialize(byteBuffer);
      byteBuffer.flip();
      request.planBytes = byteBuffer;
      request.setPlanBytesIsSet(true);

      long reqLatency = System.nanoTime();
      try {
        TSStatus status = client.executeNonQueryPlan(request);
        clientPool.returnSyncClient(client, target, ClientCategory.META);
        if (status.isSetRedirectNode()) {
          Node leader = new Node().setInternalIp(status.redirectNode.ip).setMetaPort(8880);
          endPointLeaderMap.put(endPoint, leader);
          logger.debug("Leader of {} is changed to {}", endPoint, leader);
        }

        currRequsetNum = requestCounter.incrementAndGet();
        if (currRequsetNum > threadNum * 10L) {
          reqLatency = System.nanoTime() - reqLatency;
          maxLatency = Math.max(maxLatency, reqLatency);
          latencySum.addAndGet(reqLatency);
          latencyMap.get(endPoint).add(reqLatency);
          if (duringBurst) {
            burstRequestCounter.incrementAndGet();
            burstLatencySum.addAndGet(reqLatency);
          }
        }
      } catch (TException e) {
        e.printStackTrace();
      }

      long elapsedTime = System.currentTimeMillis() - startTime;
      if (currRequsetNum % printInterval == 0) {
        System.out.println(
            String.format(
                "%s %d %d %f(%f) %f %f",
                dateFormat.format(new Date(System.currentTimeMillis())),
                elapsedTime,
                currRequsetNum,
                (currRequsetNum + 0.0) / elapsedTime,
                currRequsetNum * workloadSize / (1024.0 * 1024.0) / elapsedTime,
                maxLatency / 1000.0,
                (latencySum.get() + 0.0) / currRequsetNum));
        System.out.println(latencyMap);
      }

      if (currRequsetNum >= maxRequestNum || elapsedTime / 1000 >= maxRunningSecond) {
        break;
      }
    }
  }

  private void insertBurst() {
    long burstStart = maxRunningSecond / 2 - burstInterval / 2;
    long burstEnd = maxRunningSecond / 2 + burstInterval / 2;

    long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
    while (elapsedTime < burstStart) {
      try {
        Thread.sleep(1000);
        elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
      } catch (InterruptedException e) {
        logger.warn("Unexpected interruption");
      }
    }
    duringBurst = true;
    System.out.printf("Burst starts");
    for (Entry<EndPoint, RateLimiter> endPointRateLimiterEntry : rateLimiterMap.entrySet()) {
      RateLimiter rateLimiter = endPointRateLimiterEntry.getValue();
      rateLimiter.setRate(rateLimiter.getRate() * 2);
    }

    while (elapsedTime < burstEnd) {
      try {
        Thread.sleep(1000);
        elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
      } catch (InterruptedException e) {
        logger.warn("Unexpected interruption");
      }
    }
    duringBurst = false;
    System.out.printf("Burst ends");
    for (Entry<EndPoint, RateLimiter> endPointRateLimiterEntry : rateLimiterMap.entrySet()) {
      RateLimiter rateLimiter = endPointRateLimiterEntry.getValue();
      rateLimiter.setRate(rateLimiter.getRate() / 2);
    }
  }

  public void benchmark() {
    startTime = System.currentTimeMillis();
    for (int i = 0; i < threadNum; i++) {
      int taskId = i;
      pool.submit(() -> benchmarkTask(taskId));
    }
    pool.shutdown();
    if (burstInterval > 0) {
      insertBurst();
    }
    while (!pool.isTerminated()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("Unexpected interruption");
      }
    }
  }

  public void setMaxRequestNum(int maxRequestNum) {
    this.maxRequestNum = maxRequestNum;
  }

  public static void main(String[] args) {
    ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(50000);
    Node target = new Node();
    ExprBench bench = new ExprBench(target);
    bench.maxRunningSecond = Integer.parseInt(args[0]);
    bench.burstInterval = Integer.parseInt(args[1]);
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
    String[] raftFactorSplit = args[7].split(",");
    bench.raftFactors = new int[raftFactorSplit.length];
    for (int i = 0; i < raftFactorSplit.length; i++) {
      bench.raftFactors[i] = Integer.parseInt(raftFactorSplit[i]);
    }
    if (args.length >= 9) {
      String[] ratesSplit = args[8].split(",");
      bench.rateLimits = new int[ratesSplit.length];
      for (int i = 0; i < ratesSplit.length; i++) {
        bench.rateLimits[i] = Integer.parseInt(ratesSplit[i]);
      }
    }

    List<Node> list = bench.nodeList;
    for (int i = 0, listSize = list.size(); i < listSize; i++) {
      Node node = list.get(i);
      for (int j = 0; j < bench.raftFactors[i]; j++) {
        EndPoint endPoint = new EndPoint(node, j);
        bench.endPoints.add(endPoint);
        bench.latencyMap.put(endPoint, new Statistic());
        if (bench.rateLimits != null) {
          bench.rateLimiterMap.put(endPoint, RateLimiter.create(bench.rateLimits[i]));
        }
      }
    }

    bench.benchmark();

    System.out.println(bench.latencyMap);
    if (bench.burstInterval > 0) {
      long burstRequest = bench.burstRequestCounter.get();
      long burstLatencySum = bench.burstLatencySum.get();
      double burstAvgLatency = burstLatencySum * 1.0 / burstRequest;
      double burstThroughput = burstRequest * 1.0 / bench.burstInterval;
      System.out.printf(
          "Statistics during burst: num request %d, throughput %f, latency %f",
          burstRequest, burstThroughput, burstAvgLatency);
    }
  }
}
