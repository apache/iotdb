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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Timer {

  public static final boolean ENABLE_INSTRUMENTING = true;

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
    RAFT_SENDER_COMMIT_LOG_IN_MANAGER(
        RAFT_MEMBER_SENDER,
        "commit log in log manager",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_LOG),
    RAFT_SENDER_COMMIT_GET_LOGS(
        RAFT_MEMBER_SENDER,
        "get logs to be committed",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_DELETE_EXCEEDING_LOGS(
        RAFT_MEMBER_SENDER,
        "delete logs exceeding capacity",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_APPEND_AND_STABLE_LOGS(
        RAFT_MEMBER_SENDER,
        "append and stable committed logs",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_APPLY_LOGS(
        RAFT_MEMBER_SENDER,
        "apply after committing logs",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        RAFT_SENDER_COMMIT_LOG_IN_MANAGER),
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
    RAFT_SENDER_LOG_FROM_CREATE_TO_ACCEPT(
        RAFT_MEMBER_SENDER,
        "log from create to accept",
        TIME_SCALE,
        RaftMember.USE_LOG_DISPATCHER,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
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
    RAFT_RECEIVER_INDEX_DIFF(RAFT_MEMBER_RECEIVER, "index diff", 1.0, true, ROOT),
    // log dispatcher
    LOG_DISPATCHER_LOG_IN_QUEUE(
        LOG_DISPATCHER,
        "in queue",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_END(
        LOG_DISPATCHER,
        "from create to end",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP);

    String className;
    String blockName;
    AtomicLong sum = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);
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
    public void calOperationCostTimeFromStart(long startTime) {
      if (ENABLE_INSTRUMENTING && startTime != Long.MIN_VALUE) {
        add(System.nanoTime() - startTime);
      }
    }

    /** WARN: no current safety guarantee. */
    public void reset() {
      sum.set(0);
      counter.set(0);
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
      return String.format("%s - %s: %.2f, %d, %.2f", className, blockName, s, cnt, avg);
    }
  }

  public static String getReport() {
    if (!ENABLE_INSTRUMENTING) {
      return "";
    }
    StringBuilder result = new StringBuilder();
    printTo(Statistic.ROOT, result);
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
