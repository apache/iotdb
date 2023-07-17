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

package org.apache.iotdb.consensus.natraft.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Timer {

  private static final Logger logger = LoggerFactory.getLogger(Timer.class);

  public static final boolean ENABLE_INSTRUMENTING = true;

  private static final String COORDINATOR = "Coordinator";
  private static final String META_GROUP_MEMBER = "Meta group member";
  private static final String DATA_GROUP_MEMBER = "Data group member";
  private static final String RAFT_MEMBER_SENDER = " Raft member(sender)";
  private static final String RAFT_MEMBER_RECEIVER = " Raft member(receiver)";
  private static final String LOG_DISPATCHER = "Log dispatcher";
  private static final String ENGINE_INSERTION = "Engine insertion";

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
    RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2(
        RAFT_MEMBER_SENDER,
        "compete for log manager before append",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_OCCUPY_LOG_MANAGER_IN_APPEND(
        RAFT_MEMBER_SENDER,
        "occupy log manager in append",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_APPEND_LOG_V2(
        RAFT_MEMBER_SENDER,
        "locally append log",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_COMPRESS_LOG(
        RAFT_MEMBER_SENDER,
        "compress entries in dispatcher",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_SEND_LOG(
        RAFT_MEMBER_SENDER,
        "send log to a follower",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_BUILD_LOG_REQUEST(
        RAFT_MEMBER_SENDER,
        "build SendLogRequest",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
    RAFT_SENDER_BUILD_APPEND_REQUEST(
        RAFT_MEMBER_SENDER,
        "build AppendEntryRequest",
        TIME_SCALE,
        true,
        RAFT_SENDER_BUILD_LOG_REQUEST),
    RAFT_SENDER_OFFER_LOG(
        RAFT_MEMBER_SENDER,
        "offer log to dispatcher",
        TIME_SCALE,
        true,
        DATA_GROUP_MEMBER_LOCAL_EXECUTION),
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
        RAFT_MEMBER_SENDER, "commit log in log manager", TIME_SCALE, true, RAFT_SENDER_COMMIT_LOG),
    RAFT_SENDER_EXIT_LOG_MANAGER(
        RAFT_MEMBER_SENDER,
        "exiting log manager synchronizer",
        TIME_SCALE,
        true,
        RAFT_SENDER_COMMIT_LOG),
    RAFT_SENDER_COMMIT_GET_LOGS(
        RAFT_MEMBER_SENDER,
        "get logs to be committed",
        TIME_SCALE,
        true,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_DELETE_EXCEEDING_LOGS(
        RAFT_MEMBER_SENDER,
        "delete logs exceeding capacity",
        TIME_SCALE,
        true,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_APPEND_AND_STABLE_LOGS(
        RAFT_MEMBER_SENDER,
        "append and stable committed logs",
        TIME_SCALE,
        true,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_APPLY_LOGS(
        RAFT_MEMBER_SENDER,
        "apply after committing logs",
        TIME_SCALE,
        true,
        RAFT_COMMIT_LOG_IN_MANAGER),
    RAFT_SENDER_COMMIT_TO_CONSUMER_LOGS(
        RAFT_MEMBER_SENDER,
        "provide log to consumer",
        TIME_SCALE,
        true,
        RAFT_SENDER_COMMIT_APPLY_LOGS),
    RAFT_SENDER_COMMIT_EXCLUSIVE_LOGS(
        RAFT_MEMBER_SENDER,
        "apply logs that cannot run in parallel",
        TIME_SCALE,
        true,
        RAFT_SENDER_COMMIT_APPLY_LOGS),
    RAFT_SENDER_COMMIT_WAIT_LOG_APPLY(
        RAFT_MEMBER_SENDER, "wait until log is applied", TIME_SCALE, true, RAFT_SENDER_COMMIT_LOG),
    RAFT_SENDER_IN_APPLY_QUEUE(
        RAFT_MEMBER_SENDER, "in apply queue", TIME_SCALE, true, RAFT_SENDER_COMMIT_WAIT_LOG_APPLY),
    RAFT_SENDER_DATA_LOG_APPLY(
        RAFT_MEMBER_SENDER, "apply data log", TIME_SCALE, true, RAFT_SENDER_COMMIT_WAIT_LOG_APPLY),
    // raft member - receiver
    RAFT_RECEIVER_DECOMPRESS_ENTRY(
        RAFT_MEMBER_RECEIVER,
        "receiver decompress entries",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_PARSE_ENTRY(
        RAFT_MEMBER_RECEIVER,
        "receiver parse entries",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_WAIT_FOR_PREV_LOG(
        RAFT_MEMBER_RECEIVER,
        "receiver wait for prev log",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_WAIT_FOR_WINDOW(
        RAFT_MEMBER_RECEIVER,
        "receiver wait for window",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_WAIT_LOCK(
        RAFT_MEMBER_RECEIVER,
        "receiver wait for lock",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_WINDOW_FLUSH_SIZE(
        RAFT_MEMBER_RECEIVER,
        "receiver window flush size",
        1,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_APPEND_INTERNAL(
        RAFT_MEMBER_RECEIVER,
        "append entry (internal)",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_APPEND_ENTRIES(
        RAFT_MEMBER_RECEIVER,
        "receiver append entries",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_APPEND_ONE_ENTRY(
        RAFT_MEMBER_RECEIVER,
        "receiver append one entry",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_APPEND_ONE_ENTRY_SYNC(
        RAFT_MEMBER_RECEIVER,
        "receiver append one entry (sync)",
        TIME_SCALE,
        true,
        RAFT_RECEIVER_APPEND_ONE_ENTRY),
    RAFT_RECEIVER_UPDATE_COMMIT_INDEX(
        RAFT_MEMBER_RECEIVER,
        "receiver update commit index",
        TIME_SCALE,
        true,
        RAFT_RECEIVER_APPEND_ONE_ENTRY),
    RAFT_RECEIVER_APPEND_ENTRY(
        RAFT_MEMBER_RECEIVER,
        "append entrys",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_APPEND_ACK(
        RAFT_MEMBER_RECEIVER,
        "ack append entrys",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_RECEIVER_APPEND_ENTRY_FULL(
        RAFT_MEMBER_RECEIVER,
        "append entrys(full)",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_APPLY_BLOCK(RAFT_MEMBER_RECEIVER, "apply blocking time", TIME_SCALE, true, ROOT),
    SERIALIZE_ENTRY(
        LOG_DISPATCHER,
        "serialize entry",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
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
    RAFT_SENDER_LOG_FROM_CREATE_TO_BEFORE_COMMIT(
        LOG_DISPATCHER,
        "from create to before commit",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_GET_LOG_FOR_COMMIT(
        LOG_DISPATCHER,
        "get log for commit",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_READY_COMMIT(
        LOG_DISPATCHER,
        "from create to ready commit",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_COMMIT_HOLD_LOCK(
        LOG_DISPATCHER,
        "commit hold lock",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_COMMIT(
        LOG_DISPATCHER,
        "from create to committed",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_APPLIER(
        LOG_DISPATCHER,
        "from create to applier",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_APPLYING(
        LOG_DISPATCHER,
        "from create to applying",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_FROM_CREATE_TO_APPLIED(
        LOG_DISPATCHER,
        "from create to applied",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_NOTIFIED(
        LOG_DISPATCHER,
        "from create to notified",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_WAIT_APPEND_START(
        LOG_DISPATCHER,
        "from create to wait append start",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_APPEND_WAIT(
        LOG_DISPATCHER,
        "wait for being appended",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_WAIT_APPEND_END(
        LOG_DISPATCHER,
        "from create to wait append end",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_SENDER_LOG_FROM_CREATE_TO_WAIT_APPLY_END(
        LOG_DISPATCHER,
        "from create to wait apply end",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_TOTAL(
        LOG_DISPATCHER,
        "total process time",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_RAW_SIZE(
        LOG_DISPATCHER,
        "raw dispatching size",
        1,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_COMPRESSED_SIZE(
        LOG_DISPATCHER,
        "compressed dispatching size",
        1,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    LOG_DISPATCHER_BATCH_SIZE(
        LOG_DISPATCHER,
        "batch dispatching size",
        1,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    PERSISTENCE_COMPRESSED_SIZE(
        LOG_DISPATCHER,
        "compressed persistence size",
        1,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    PERSISTENCE_RAW_SIZE(
        LOG_DISPATCHER,
        "raw persistence size",
        1,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    PERSISTENCE_COMPRESS_TIME(
        LOG_DISPATCHER,
        "persistence compression time",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    PERSISTENCE_IO_TIME(
        LOG_DISPATCHER,
        "persistence IO time",
        TIME_SCALE,
        true,
        META_GROUP_MEMBER_EXECUTE_NON_QUERY_IN_LOCAL_GROUP),
    RAFT_LEADER_WEAK_ACCEPT(RAFT_MEMBER_SENDER, "leader weak accept", 1, true, ROOT),
    RAFT_FOLLOWER_WEAK_ACCEPT(RAFT_MEMBER_SENDER, "follower weak accept", TIME_SCALE, true, ROOT),
    RAFT_PUT_LOG(RAFT_MEMBER_SENDER, "put logs", TIME_SCALE, true, ROOT),
    RAFT_PUT_ENTRY(RAFT_MEMBER_SENDER, "put one entry", TIME_SCALE, true, ROOT),
    RAFT_FOLLOWER_STRONG_ACCEPT(
        RAFT_MEMBER_SENDER, "follower strong accept", TIME_SCALE, true, ROOT),
    LOG_WRITE_LOCK("Lock", "write lock", TIME_SCALE, true, ROOT),
    LOG_WRITE_UNLOCK("Lock", "write unlock", TIME_SCALE, true, ROOT),
    LOG_READ_LOCK("Lock", "read lock", TIME_SCALE, true, ROOT),
    LOG_READ_UNLOCK("Lock", "read unlock", TIME_SCALE, true, ROOT),
    ENGINE_INSERT_TABLET(ENGINE_INSERTION, "insert tablet", TIME_SCALE, true, ROOT),
    ENGINE_INSERT_TABLET_LOCK(
        ENGINE_INSERTION, "insert tablet: lock", TIME_SCALE, true, ENGINE_INSERT_TABLET),
    ENGINE_INSERT_TABLET_LOOP(
        ENGINE_INSERTION, "insert tablet: loop", TIME_SCALE, true, ENGINE_INSERT_TABLET),
    ENGINE_INSERT_TABLET_PROCESSOR(
        ENGINE_INSERTION, "insert tablet: processor", TIME_SCALE, true, ENGINE_INSERT_TABLET_LOOP),
    ENGINE_INSERT_TABLET_MEMCON(
        ENGINE_INSERTION,
        "insert tablet: memory control",
        TIME_SCALE,
        true,
        ENGINE_INSERT_TABLET_LOOP),
    ENGINE_INSERT_TABLET_WAL(
        ENGINE_INSERTION, "insert tablet: wal flush", TIME_SCALE, true, ENGINE_INSERT_TABLET_LOOP),
    ENGINE_INSERT_TABLET_MEMTABLE(
        ENGINE_INSERTION, "insert tablet: MemTable", TIME_SCALE, true, ENGINE_INSERT_TABLET_LOOP);

    String className;
    String blockName;
    AtomicLong sum = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);
    AtomicLong intervalSum = new AtomicLong(0);
    AtomicLong intervalCounter = new AtomicLong(0);
    long max;
    long intervalMax;
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
        intervalSum.addAndGet(val);
        intervalCounter.incrementAndGet();
        max = Math.max(max, val);
        intervalMax = Math.max(intervalMax, val);
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
      intervalCounter.set(0);
      intervalSum.set(0);
      intervalMax = 0;
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
      double intervalS = intervalSum.get() / scale;
      long intervalCnt = intervalCounter.get();
      double avg = s / cnt;
      double intervalAvg = intervalS / intervalCnt;
      intervalSum.set(0);
      intervalCounter.set(0);
      intervalMax = 0;
      return String.format(
          "%s - %s: %.4f(%.4f), %d(%d), %.4f(%.4f), %d(%d)",
          className, blockName, s, intervalS, cnt, intervalCnt, avg, intervalAvg, max, intervalMax);
    }

    public long getCnt() {
      return counter.get();
    }

    public long getSum() {
      return sum.get();
    }

    public static String getReport() {
      if (!ENABLE_INSTRUMENTING) {
        return "";
      }
      StringBuilder result = new StringBuilder("\n");
      printTo(Statistic.ROOT, result);
      result
          .append("Dispatcher compression ratio: ")
          .append(LOG_DISPATCHER_COMPRESSED_SIZE.getSum() * 1.0 / LOG_DISPATCHER_RAW_SIZE.getSum())
          .append("\n");
      result
          .append("Persistence compression ratio: ")
          .append(PERSISTENCE_COMPRESSED_SIZE.getSum() * 1.0 / PERSISTENCE_RAW_SIZE.getSum())
          .append("\n");
      return result.toString();
    }

    private static void printTo(Statistic currNode, StringBuilder out) {
      if (currNode != Statistic.ROOT && currNode.valid) {
        if (currNode.counter.get() != 0) {
          indent(out, currNode.level);
          out.append(currNode).append("\n");
        }
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
}
