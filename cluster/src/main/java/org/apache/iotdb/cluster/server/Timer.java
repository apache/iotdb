package org.apache.iotdb.cluster.server;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;

public class Timer {

  public static Statistic dataGroupMemberProcessPlanLocally = new Statistic(
      "Data group member - process plan locally: ", 1000000L);
  public static Statistic dataGroupMemberWaitLeader = new Statistic(
      "Data group member - wait leader: ", 1000000L);
  public static Statistic metaGroupMemberExecuteNonQuery = new Statistic(
      "Meta group member - execute non query: ", 1000000L);
  public static Statistic metaGroupMemberExecuteNonQueryInLocalGroup = new Statistic(
      "Meta group member - execute in local group: ", 1000000L);
  public static Statistic metaGroupMemberExecuteNonQueryInRemoteGroup = new Statistic(
      "Meta group member - execute in remote group: ", 1000000L);
  public static Statistic raftMemberAppendLog = new Statistic("Raft member - append log: ",
      1000000L);
  public static Statistic raftMemberSendLogToFollower = new Statistic(
      "Raft member - send log to follower: ", 1000000L);
  public static Statistic raftMemberCommitLog = new Statistic("Raft member - commit log: ",
      1000000L);
  public static Statistic raftFollowerAppendEntry = new Statistic(
      "Raft member - follower append entry: ", 1000000L);
  public static Statistic dataGroupMemberForwardPlan = new Statistic(
      "Data group member - forward plan: ", 1000000L);
  public static Statistic raftMemberWaitForPrevLog = new Statistic(
      "Raft member - wait for prev log: ", 1000000L);
  public static Statistic raftMemberSendLogAync = new Statistic("Raft member - send log aync: ",
      1000000L);
  public static Statistic raftMemberVoteCounter = new Statistic("Raft member - vote counter: ",
      1000000L);
  public static Statistic raftMemberLogParse = new Statistic("Raft member - log parse: ", 1000000L);
  public static Statistic rafTMemberReceiverWaitForPrevLog = new Statistic(
      "Raft member - receiver wait for prev log: ", 1000000L);
  public static Statistic rafTMemberMayBeAppend = new Statistic("rafTMemberMayBeAppendMS",
      1000000L);
  public static Statistic raftMemberOfferLog = new Statistic("Raft member - offer log: ", 1000000L);
  public static Statistic raftMemberCommitLogResult = new Statistic(
      "aft member - commit log result: ", 1000000L);
  public static Statistic raftMemberAppendLogResult = new Statistic(
      "Raft member - append log result: ", 1000000L);
  public static Statistic indexDiff = new Statistic("Raft member - index diff: ", 1L);
  public static Statistic logDispatcherLogInQueue = new Statistic("Log dispatcher - in queue: ",
      1000000L);
  public static Statistic raftMemberFromCreateToAppendLog = new Statistic("Raft member - from create to append log: ", 1000000L);
  public static Statistic logDispatcherFromCreateToEnd= new Statistic("Log dispatcher - from create to end: ", 1000000L);

  public static int[] queueHisto = new int[21];
  public static int[] currentBatchHisto = new int[21];

  static Statistic[] statistics = new Statistic[]{dataGroupMemberProcessPlanLocally,
      dataGroupMemberWaitLeader,
      metaGroupMemberExecuteNonQuery,
      metaGroupMemberExecuteNonQueryInLocalGroup,
      metaGroupMemberExecuteNonQueryInRemoteGroup,
      raftMemberAppendLog,
      raftMemberSendLogToFollower,
      raftMemberCommitLog,
      raftFollowerAppendEntry,
      dataGroupMemberForwardPlan,
      raftMemberWaitForPrevLog,
      raftMemberSendLogAync,
      raftMemberVoteCounter,
      raftMemberLogParse,
      rafTMemberReceiverWaitForPrevLog,
      rafTMemberMayBeAppend,
      raftMemberOfferLog,
      raftMemberCommitLogResult,
      raftMemberAppendLogResult,
      indexDiff,
      logDispatcherLogInQueue,
      raftMemberFromCreateToAppendLog,
      logDispatcherFromCreateToEnd};

  public static class Statistic {

    String name;
    AtomicLong sum = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);
    long scale;

    Statistic(String name, long scale) {
      this.name = name;
      this.scale = scale;
    }

    public void add(long val) {
      sum.addAndGet(val);
      counter.incrementAndGet();
    }

    @Override
    public String toString() {
      return name
          + sum.get() / scale + ", "
          + counter + ", "
          + (double) sum.get() / scale
          / counter.get();
    }
  }

  public static String getReport() {
    String result = "\n";
    for (Statistic s : statistics) {
      result += s.toString() + "\n";
    }
    result += Arrays.toString(queueHisto) + "\n";
    result += Arrays.toString(currentBatchHisto) + "\n";
    return result;
  }
}
