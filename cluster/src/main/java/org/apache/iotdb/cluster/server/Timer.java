package org.apache.iotdb.cluster.server;

import java.util.concurrent.atomic.AtomicLong;

public class Timer {

  public static AtomicLong dataGroupMemberProcessPlanLocallyMS = new AtomicLong(0);
  public static AtomicLong dataGroupMemberProcessPlanLocallyCounter = new AtomicLong(0);
  public static AtomicLong dataGroupMemberWaitLeaderMS = new AtomicLong(0);
  public static AtomicLong dataGroupMemberWaitLeaderCounter = new AtomicLong(0);
  public static AtomicLong metaGroupMemberExecuteNonQueryMS = new AtomicLong(0);
  public static AtomicLong metaGroupMemberExecuteNonQueryCounter = new AtomicLong(0);
  public static AtomicLong metaGroupMemberExecuteNonQueryInLocalGroupMS = new AtomicLong(0);
  public static AtomicLong metaGroupMemberExecuteNonQueryInLocalGroupCounter = new AtomicLong(0);
  public static AtomicLong metaGroupMemberExecuteNonQueryInRemoteGroupMS = new AtomicLong(0);
  public static AtomicLong metaGroupMemberExecuteNonQueryInRemoteGroupCounter = new AtomicLong(0);
  public static AtomicLong raftMemberAppendLogMS = new AtomicLong(0);
  public static AtomicLong raftMemberAppendLogCounter = new AtomicLong(0);
  public static AtomicLong raftMemberSendLogToFollowerMS = new AtomicLong(0);
  public static AtomicLong raftMemberSendLogToFollowerCounter = new AtomicLong(0);
  public static AtomicLong raftMemberCommitLogMS = new AtomicLong(0);
  public static AtomicLong raftMemberCommitLogCounter = new AtomicLong(0);
  public static AtomicLong raftFollowerAppendEntryMS = new AtomicLong(0);
  public static AtomicLong raftFollowerAppendEntryCounter = new AtomicLong(0);
  public static AtomicLong dataGroupMemberForwardPlanMS = new AtomicLong(0);
  public static AtomicLong dataGroupMemberForwardPlanCounter = new AtomicLong(0);
  public static AtomicLong raftMemberWaitForPrevLogMS = new AtomicLong(0);
  public static AtomicLong raftMemberWaitForPrevLogCounter = new AtomicLong(0);
  public static AtomicLong raftMemberSendLogAyncMS = new AtomicLong(0);
  public static AtomicLong raftMemberSendLogAyncCounter = new AtomicLong(0);
  public static AtomicLong raftMemberVoteCounterMS = new AtomicLong(0);
  public static AtomicLong raftMemberVoteCounterCounter = new AtomicLong(0);
  public static AtomicLong raftMemberLogParseMS = new AtomicLong(0);
  public static AtomicLong raftMemberLogParseCounter = new AtomicLong(0);
  public static AtomicLong rafTMemberReceiverWaitForPrevLogMS = new AtomicLong(0);
  public static AtomicLong rafTMemberReceiverWaitForPrevLogCounter = new AtomicLong(0);
  public static AtomicLong rafTMemberMayBeAppendMS = new AtomicLong(0);
  public static AtomicLong rafTMemberMayBeAppendCounter = new AtomicLong(0);
  public static AtomicLong raftMemberOfferLogMS = new AtomicLong(0);
  public static AtomicLong raftMemberOfferLogCounter = new AtomicLong(0);
  public static AtomicLong raftMemberCommitLogResultMS = new AtomicLong(0);
  public static AtomicLong raftMemberCommitLogResultCounter = new AtomicLong(0);
  public static AtomicLong raftMemberAppendLogResultMS = new AtomicLong(0);
  public static AtomicLong raftMemberAppendLogResultCounter = new AtomicLong(0);
//  public static AtomicLong indexDiff = new AtomicLong(0);
  public static AtomicLong indexDiffCounter = new AtomicLong(0);
  public static AtomicLong logDispatcherLogInQueueMS = new AtomicLong(0);
  public static AtomicLong logDispatcherLogInQueueCounter = new AtomicLong(0);

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
    result += dataGroupMemberProcessPlanLocally.toString() + "\n";
    result += dataGroupMemberWaitLeader.toString() + "\n";
    result += metaGroupMemberExecuteNonQuery.toString() + "\n";
    result += metaGroupMemberExecuteNonQueryInLocalGroup.toString() + "\n";
    result += metaGroupMemberExecuteNonQueryInRemoteGroup.toString() + "\n";
    result += raftMemberAppendLog.toString() + "\n";
    result += raftMemberSendLogToFollower.toString() + "\n";
    result += raftMemberCommitLog.toString() + "\n";
    result += raftFollowerAppendEntry.toString() + "\n";
    result += dataGroupMemberForwardPlan.toString() + "\n";
    result += raftMemberWaitForPrevLog.toString() + "\n";
    result += raftMemberSendLogAync.toString() + "\n";
    result += raftMemberVoteCounter.toString() + "\n";
    result += raftMemberLogParse.toString() + "\n";
    result += rafTMemberReceiverWaitForPrevLog.toString() + "\n";
    result += rafTMemberMayBeAppend.toString() + "\n";
    result += raftMemberOfferLog.toString() + "\n";
    result += raftMemberAppendLogResult.toString() + "\n";
    result += raftMemberCommitLogResult.toString() + "\n";
    result += logDispatcherLogInQueue.toString() + "\n";
    result += indexDiff.toString() + "\n";
    return result;
  }
}
