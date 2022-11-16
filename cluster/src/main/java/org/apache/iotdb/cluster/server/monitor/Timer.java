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

package org.apache.iotdb.cluster.server.monitor;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.WindowStatistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Timer {

  private static final Logger logger = LoggerFactory.getLogger(Timer.class);

  public static final boolean ENABLE_INSTRUMENTING =
      ClusterDescriptor.getInstance().getConfig().isEnableInstrumenting();

  private static final String COORDINATOR = "Coordinator";
  private static final String META_GROUP_MEMBER = "Meta group member";
  private static final String DATA_GROUP_MEMBER = "Data group member";
  private static final String RAFT_MEMBER_SENDER = " Raft member(sender)";
  private static final String RAFT_MEMBER_RECEIVER = " Raft member(receiver)";
  private static final String LOG_DISPATCHER = "Log dispatcher";

  // convert nano to milli
  private static final double TIME_SCALE = 1_000_000.0;

  public enum Statistic {
    // A dummy root for the convenience of prints
    ROOT("ClassName", "BlockName", TIME_SCALE, true, null),
    // coordinator
    COORDINATOR_EXECUTE_NON_QUERY(COORDINATOR, "execute non query", TIME_SCALE, true, ROOT),

    // meta group member
    META_GROUP_MEMBER_EXECUTE_NON_QUERY(
        META_GROUP_MEMBER, "execute non query", TIME_SCALE, true, COORDINATOR_EXECUTE_NON_QUERY),
    META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP(
        META_GROUP_MEMBER,
        "execute in local group",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY),
    META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_REMOTE_GROUP(
        META_GROUP_MEMBER,
        "execute in remote group",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY),
    // data group member
    DATA_GROUP_MEMBER_LOCAL_EXECUTION(
        DATA_GROUP_MEMBER,
        "execute locally",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    DATA_GROUP_MEMBER_WAIT_LEADER(
        DATA_GROUP_MEMBER,
        "wait for leader",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    DATA_GROUP_MEMBER_FORWARD_PLAN(
        DATA_GROUP_MEMBER,
        "forward to leader",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    // raft member - sender
    RAFT_SENDER_SEQUENCE_LOG(
        RAFT_MEMBER_SENDER, "sequence log", TIME_SCALE, true, META_GROUP_MEMBER_EXECUTE_NON_QUERY),
    RAFT_SENDER_APPEND_LOG(
        RAFT_MEMBER_SENDER,
        "locally append log",
        TIME_SCALE,
        !RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2(
        RAFT_MEMBER_SENDER,
        "compete for log manager before append",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_OCCUPY_LOG_MANAGER_IN_APPEND(
        RAFT_MEMBER_SENDER,
        "occupy log manager in append",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_APPEND_LOG_V2(
        RAFT_MEMBER_SENDER,
        "locally append log",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_BUILD_LOG_REQUEST(
        RAFT_MEMBER_SENDER,
        "build SendLogRequest",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_BUILD_APPEND_REQUEST(
        RAFT_MEMBER_SENDER,
        "build AppendEntryRequest",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_BUILD_LOG_REQUEST),
    RAFT_SENDER_OFFER_LOG(
        RAFT_MEMBER_SENDER,
        "offer log to dispatcher",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_SEND_LOG_TO_FOLLOWERS(
        RAFT_MEMBER_SENDER,
        "send log to followers",
        TIME_SCALE,
        !RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_WAIT_FOR_PREV_LOG(
        RAFT_MEMBER_SENDER,
        "sender wait for prev log",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_SENDER_SERIALIZE_LOG(
        RAFT_MEMBER_SENDER, "serialize logs", TIME_SCALE, true, RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_SENDER_SEND_LOG_ASYNC(
        RAFT_MEMBER_SENDER,
        "send log async",
        TIME_SCALE,
        ClusterDescriptor.getInstance().getConfig().isUseAsyncServer(),
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_SENDER_SEND_LOG(
        RAFT_MEMBER_SENDER, "send log", TIME_SCALE, true, RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_SENDER_HANDLE_SEND_RESULT(
        RAFT_MEMBER_SENDER,
        "handle send log result",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_RELAY_OFFER_LOG(
        RAFT_MEMBER_RECEIVER,
        "relay offer log",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_RELAY_LOG(
        RAFT_MEMBER_RECEIVER,
        "relay entry to a follower",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_SENDER_VOTE_COUNTER(
        RAFT_MEMBER_SENDER,
        "wait for votes",
        TIME_SCALE,
        true,
        RaftMember.USE_LOG_DISPATCHER
            ? DATA_GROUP_MEMBER_LOCAL_EXECUTION
            : RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_SENDER_COMMIT_LOG(
        RAFT_MEMBER_SENDER,
        "locally commit log",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_COMMIT(
        RAFT_MEMBER_SENDER,
        "compete for log manager before commit",
        TIME_SCALE,
        true,
        RAFT_SENDER_COMMIT_LOG),
    RAFT_COMMIT_LOG_IN_MANAGER(
        RAFT_MEMBER_SENDER,
        "commit log in log manager",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_LOG),
    RAFT_SENDER_EXIT_LOG_MANAGER(
        RAFT_MEMBER_SENDER,
        "exiting log manager synchronizer",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_LOG),
    RAFT_SENDER_COMMIT_GET_LOGS(
        RAFT_MEMBER_SENDER,
        "get logs to be committed",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_DELETE_EXCEEDING_LOGS(
        RAFT_MEMBER_SENDER,
        "delete logs exceeding capacity",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_APPEND_AND_STABLE_LOGS(
        RAFT_MEMBER_SENDER,
        "append and stable committed logs",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_APPLY_LOGS(
        RAFT_MEMBER_SENDER,
        "apply after committing logs",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_TO_CONSUMER_LOGS(
        RAFT_MEMBER_SENDER,
        "provide log to consumer",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_APPLY_LOGS),
    RAFT_SENDER_COMMIT_EXCLUSIVE_LOGS(
        RAFT_MEMBER_SENDER,
        "apply logs that cannot run in parallel",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_APPLY_LOGS),
    RAFT_SENDER_COMMIT_WAIT_LOG_APPLY(
        RAFT_MEMBER_SENDER, "wait until log is applied", TIME_SCALE, true, RAFT_SENDER_COMMIT_LOG),
    RAFT_SENDER_IN_APPLY_QUEUE(
        RAFT_MEMBER_SENDER, "in apply queue", TIME_SCALE, true, RAFT_SENDER_COMMIT_WAIT_LOG_APPLY),
    RAFT_SENDER_DATA_LOG_APPLY(
        RAFT_MEMBER_SENDER, "apply data log", TIME_SCALE, true, RAFT_SENDER_COMMIT_WAIT_LOG_APPLY),
    // raft member - receiver
    RAFT_RECEIVER_LOG_PARSE(
        RAFT_MEMBER_RECEIVER, "log parse", TIME_SCALE, true, RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_WAIT_FOR_PREV_LOG(
        RAFT_MEMBER_RECEIVER,
        "receiver wait for prev log",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_APPEND_ENTRY(
        RAFT_MEMBER_RECEIVER, "append entrys", TIME_SCALE, true, RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_APPEND_ACK(
        RAFT_MEMBER_RECEIVER,
        "ack append entrys",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_APPEND_ENTRY_FULL(
        RAFT_MEMBER_RECEIVER,
        "append entrys(full)",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_HANDLE_APPEND_ACK(
        RAFT_MEMBER_SENDER,
        "handle append entrys ack",
        TIME_SCALE,
        true,
        RAFT_SENDER_SEND_LOG_TO_FOLLOWERS),
    RAFT_RECEIVER_INDEX_DIFF(RAFT_MEMBER_RECEIVER, "index diff", 1.0, true, ROOT),
    // log dispatcher
    LOG_DISPATCHER_LOG_ENQUEUE(
        LOG_DISPATCHER,
        "enqueue",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_LOG_ENQUEUE_SINGLE(
        LOG_DISPATCHER,
        "enqueue (single)",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_LOG_IN_QUEUE(
        LOG_DISPATCHER,
        "in queue",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_LOG_BATCH_SIZE(
        LOG_DISPATCHER, "batch size", 1, true, META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_RECEIVE_TO_CREATE(
        LOG_DISPATCHER,
        "from receive to create",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_ENQUEUE(
        LOG_DISPATCHER,
        "from create to queue",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_DEQUEUE(
        LOG_DISPATCHER,
        "from create to dequeue",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_SENDING(
        LOG_DISPATCHER,
        "from create to sending",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_SENT(
        LOG_DISPATCHER,
        "from create to sent",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_ACCEPT(
        LOG_DISPATCHER,
        "from create to accept",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_COMMIT(
        LOG_DISPATCHER,
        "from create to commit",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_WAIT_END(
        LOG_DISPATCHER,
        "from create to wait end",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_OK(
        LOG_DISPATCHER,
        "from create to OK",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_TOTAL(
        LOG_DISPATCHER,
        "total process time",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_WINDOW_LENGTH(RAFT_MEMBER_RECEIVER, "window length", 1, true, ROOT),
    RAFT_RELAYED_ENTRY(RAFT_MEMBER_RECEIVER, "number of relayed entries", 1, true, ROOT),
    RAFT_SEND_RELAY_ACK(RAFT_MEMBER_RECEIVER, "send relay ack", 1, true, ROOT),
    RAFT_SENT_ENTRY_SIZE(RAFT_MEMBER_SENDER, "sent entry size", 1, true, ROOT),
    DISPATCHER_QUEUE_LENGTH(RAFT_MEMBER_SENDER, "dispatcher queue length", 1, true, ROOT),
    RAFT_RELAYED_LEVEL1_NUM(RAFT_MEMBER_SENDER, "level 1 relay node number", 1, true, ROOT),
    RAFT_RECEIVE_RELAY_ACK(RAFT_MEMBER_SENDER, "receive relay ack", 1, true, ROOT),
    RAFT_SENDER_OOW(RAFT_MEMBER_SENDER, "out of window", 1, true, ROOT),
    RAFT_WEAK_ACCEPT(RAFT_MEMBER_SENDER, "weak accept", 1, true, ROOT),
    RAFT_CONCURRENT_SENDER(RAFT_MEMBER_SENDER, "concurrent sender", 1, true, ROOT),
    RAFT_INDEX_BLOCKER(RAFT_MEMBER_SENDER, "index blocker", 1, true, ROOT),
    RAFT_APPEND_BLOCKER(RAFT_MEMBER_SENDER, "append blocker", 1, true, ROOT),
    RAFT_APPLY_BLOCKER(RAFT_MEMBER_SENDER, "apply blocker", 1, true, ROOT);

    String className;
    String blockName;
    AtomicLong sum = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);
    private WindowStatistic latestWindow = new WindowStatistic();
    long max;
    double scale;
    boolean valid;
    int level;
    Statistic parent;
    List<Statistic> children = new ArrayList<>();

    Statistic(String className, String blockName, double scale, boolean valid, Statistic parent) {
      this.className = className;
      this.blockName = blockName;
      this.scale = scale;
      this.valid = valid;
      this.parent = parent;
      if (parent == null) {
        level = -1;
      } else {
        level = parent.level + 1;
        parent.children.add(this);
      }
    }

    public void add(long val) {
      if (ENABLE_INSTRUMENTING) {
        sum.addAndGet(val);
        counter.incrementAndGet();
        max = Math.max(max, val);
        latestWindow.add(val);
      }
    }

    /** @return System.nanoTime() if the ENABLE_INSTRUMENTING is true, else zero */
    public long getOperationStartTime() {
      if (ENABLE_INSTRUMENTING) {
        return System.nanoTime();
      }
      return Long.MIN_VALUE;
    }

    /**
     * This method equals `add(System.nanoTime() - start)`. We wrap `System.nanoTime()` in this
     * method to avoid unnecessary calls when instrumenting is disabled.
     */
    public long calOperationCostTimeFromStart(long startTime) {
      if (ENABLE_INSTRUMENTING && startTime != Long.MIN_VALUE && startTime != 0) {
        long consumed = System.nanoTime() - startTime;
        add(consumed);
        return consumed;
      }
      return 0;
    }

    /** WARN: no current safety guarantee. */
    public void reset() {
      sum.set(0);
      counter.set(0);
      max = 0;
      latestWindow.reset();
    }

    /** WARN: no current safety guarantee. */
    public static void resetAll() {
      for (Statistic value : values()) {
        value.reset();
      }
    }

    @Override
    public String toString() {
      double s = sum.get() / scale;
      long cnt = counter.get();
      double avg = s / cnt;
      return String.format(
          "%s - %s: %.2f, %d, %.2f, %d, %.2f",
          className, blockName, s, cnt, avg, max, latestWindow.getAvg());
    }

    public long getCnt() {
      return counter.get();
    }

    public long getSum() {
      return sum.get();
    }

    public static long getTotalFanout() {
      return Statistic.RAFT_SENDER_SEND_LOG.getCnt() + Statistic.RAFT_RECEIVER_RELAY_LOG.getCnt();
    }

    public static double getSendLatency() {
      return (Statistic.RAFT_SENDER_SEND_LOG.getSum() + Statistic.RAFT_RECEIVER_RELAY_LOG.getSum())
          * 1.0
          / (Statistic.RAFT_SENDER_SEND_LOG.getCnt() + Statistic.RAFT_RECEIVER_RELAY_LOG.getCnt());
    }
  }

  public static String getReport() {
    if (!ENABLE_INSTRUMENTING) {
      return "";
    }
    StringBuilder result = new StringBuilder();
    printTo(Statistic.ROOT, result);
    result.append(System.lineSeparator());
    result.append(
        String.format(
            "Total request fanout: %d, send entry latency: %f",
            Statistic.getTotalFanout(), Statistic.getSendLatency()));
    return result.toString();
  }

  private static void printTo(Statistic currNode, StringBuilder out) {
    if (currNode != Statistic.ROOT && currNode.valid) {
      indent(out, currNode.level);
      out.append(currNode).append("\n");
    }
    for (Statistic child : currNode.children) {
      printTo(child, out);
    }
  }

  private static void indent(StringBuilder out, int indents) {
    for (int i = 0; i < indents; i++) {
      out.append("  ");
    }
  }
}
