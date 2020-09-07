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
  public static AtomicLong indexDiff = new AtomicLong(0);
  public static AtomicLong indexDiffCounter = new AtomicLong(0);
  public static AtomicLong logDispatcherLogInQueueMS = new AtomicLong(0);
  public static AtomicLong logDispatcherLogInQueueCounter = new AtomicLong(0);

  class Statistic {

    String name;
    AtomicLong sum = new AtomicLong(0);
    AtomicLong counter = new AtomicLong(0);
    long scale;

    Statistic(String name, long scale) {
      this.name = name;
      this.scale = scale;
    }

    void add(long val) {
      sum.addAndGet(val);
      counter.incrementAndGet();
    }

    @Override
    public String toString() {
      return getOneLine(name, sum, counter, scale);
    }
  }


  private static final String dataGroupMemberProcessPlanLocallyMSString = "Data group member - process plan locally : ";
  private static final String dataGroupMemberWaitLeaderMSString = "Data group member - wait leader: ";
  private static final String metaGroupMemberExecuteNonQueryMSString = "Meta group member - execute non query: ";
  private static final String metaGroupMemberExecuteNonQueryInLocalGroupMSString = "Meta group member - execute in local group: ";
  private static final String metaGroupMemberExecuteNonQueryInRemoteGroupMSString = "Meta group member - execute in remote group: ";
  private static final String raftMemberAppendLogMSString = "Raft member - append log: ";
  private static final String raftMemberSendLogToFollowerMSString = "Raft member - send log to follower: ";
  private static final String raftMemberCommitLogMSString = "Raft member - commit log: ";
  private static final String raftFollowerAppendEntryString = "Raft member - follower append entry: ";
  private static final String dataGroupMemberForwardPlanString = "Data group member - forward plan: ";
  private static final String raftMemberWaitForPrevLogString = "Raft member - wait for prev log: ";
  private static final String raftMemberSendLogAyncString = "Raft member - send log aync: ";
  private static final String raftMemberVoteCounterString = "Raft member - vote counter: ";
  private static final String raftMemberLogParseString = "Raft member - log parse: ";
  private static final String rafTMemberReceiverWaitForPrevLogString = "Raft member - receiver wait for prev log: ";
  private static final String rafTMemberMayBeAppendString = "Raft member - maybe append: ";
  private static final String rafTMemberOfferLogString = "Raft member - offer log: ";
  private static final String raftMemberCommitLogResultString = "Raft member - commit log result: ";
  private static final String raftMemberAppendLogResultString = "Raft member - append log result: ";
  private static final String indexDiffString = "Raft member - index diff: ";
  private static final String logDispatcherLogInQueueString = "Log dispatcher - in queue: ";


  public static String getOneLine(String name, AtomicLong period, AtomicLong counter, long scale) {
    return name
        + period.get() / scale + ", "
        + counter + ", "
        + (double) period.get() / scale
        / counter.get();
  }

  public static String getReport() {
    String result = "\n";
    result +=
        getOneLine(dataGroupMemberProcessPlanLocallyMSString, dataGroupMemberProcessPlanLocallyMS,
            dataGroupMemberProcessPlanLocallyCounter, 1000000L)
            + "\n";
    result += getOneLine(dataGroupMemberWaitLeaderMSString, dataGroupMemberWaitLeaderMS,
        dataGroupMemberWaitLeaderCounter, 1000000L)
        + "\n";
    result += getOneLine(metaGroupMemberExecuteNonQueryMSString, metaGroupMemberExecuteNonQueryMS,
        metaGroupMemberExecuteNonQueryCounter, 1000000L)
        + "\n";
    result += getOneLine(metaGroupMemberExecuteNonQueryInLocalGroupMSString,
        metaGroupMemberExecuteNonQueryInLocalGroupMS,
        metaGroupMemberExecuteNonQueryInLocalGroupCounter, 1000000L)
        + "\n";
    result += getOneLine(metaGroupMemberExecuteNonQueryInRemoteGroupMSString,
        metaGroupMemberExecuteNonQueryInRemoteGroupMS,
        metaGroupMemberExecuteNonQueryInRemoteGroupCounter, 1000000L)
        + "\n";
    result +=
        getOneLine(raftMemberAppendLogMSString, raftMemberAppendLogMS, raftMemberAppendLogCounter, 1000000L)
            + "\n";
    result += getOneLine(raftMemberSendLogToFollowerMSString, raftMemberSendLogToFollowerMS,
        raftMemberSendLogToFollowerCounter,1000000L)
        + "\n";
    result +=
        getOneLine(raftMemberCommitLogMSString, raftMemberCommitLogMS, raftMemberCommitLogCounter,1000000L)
            + "\n";
    result += getOneLine(raftFollowerAppendEntryString, raftFollowerAppendEntryMS,
        raftFollowerAppendEntryCounter, 1000000L)
        + "\n";
    result += getOneLine(dataGroupMemberForwardPlanString, dataGroupMemberForwardPlanMS,
        dataGroupMemberForwardPlanCounter, 1000000L)
        + "\n";
    result += getOneLine(raftMemberWaitForPrevLogString, raftMemberWaitForPrevLogMS,
        raftMemberWaitForPrevLogCounter, 1000000L)
        + "\n";
    result += getOneLine(raftMemberSendLogAyncString, raftMemberSendLogAyncMS,
        raftMemberSendLogAyncCounter,1000000L)
        + "\n";
    result += getOneLine(raftMemberVoteCounterString, raftMemberVoteCounterMS,
        raftMemberVoteCounterCounter, 1000000L)
        + "\n";
    result += getOneLine(raftMemberLogParseString, raftMemberLogParseMS, raftMemberLogParseCounter, 1000000L)
        + "\n";
    result += getOneLine(rafTMemberReceiverWaitForPrevLogString, rafTMemberReceiverWaitForPrevLogMS,
        rafTMemberReceiverWaitForPrevLogCounter, 1000000L)
        + "\n";
    result += getOneLine(rafTMemberMayBeAppendString, rafTMemberMayBeAppendMS,
        rafTMemberMayBeAppendCounter, 1000000L)
        + "\n";
    result += getOneLine(rafTMemberOfferLogString, raftMemberOfferLogMS, raftMemberOfferLogCounter, 1000000L)
        + "\n";
    result += getOneLine(raftMemberAppendLogResultString, raftMemberAppendLogResultMS,
        raftMemberAppendLogResultCounter, 1000000L)
        + "\n";
    result += getOneLine(raftMemberCommitLogResultString, raftMemberCommitLogResultMS,
        raftMemberCommitLogResultCounter, 1000000L)
        + "\n";
    result += getOneLine(logDispatcherLogInQueueString, logDispatcherLogInQueueMS,
        logDispatcherLogInQueueCounter, 1000000L)
        + "\n";
    result += indexDiffString
        + indexDiff.get() + ", "
        + indexDiffCounter + ", "
        + (double) indexDiff.get() / indexDiffCounter.get() + "\n";
    return result;
  }
}
