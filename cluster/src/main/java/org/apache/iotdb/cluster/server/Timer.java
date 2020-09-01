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

  private static final String dataGroupMemberProcessPlanLocallyMSString = "Data group member - process plan locally : ";
  private static final String dataGroupMemberWaitLeaderMSString = "Data group member - wait leader: ";
  private static final String metaGroupMemberExecuteNonQueryMSString = "Meta group member - execute non query: ";
  private static final String metaGroupMemberExecuteNonQueryInLocalGroupMSString = "Meta group member - execute in local group: ";
  private static final String metaGroupMemberExecuteNonQueryInRemoteGroupMSString = "Meta group member - execute in remote group: ";
  private static final String raftMemberAppendLogMSString = "Raft member - append log: ";
  private static final String raftMemberSendLogToFollowerMSString = "Raft member - send log to follower: ";
  private static final String raftMemberCommitLogMSString = "Raft member - commit log: ";
  private static final String raftFollowerAppendEntryString = "Raft member - follower append entry: ";

  public static String getReport() {
    String result = "";
    result += dataGroupMemberProcessPlanLocallyMSString
        + dataGroupMemberProcessPlanLocallyMS + ", "
        + dataGroupMemberProcessPlanLocallyCounter + ", "
        + (double) dataGroupMemberProcessPlanLocallyMS.get()
        / dataGroupMemberProcessPlanLocallyCounter.get()
        + "\n";
    result += dataGroupMemberWaitLeaderMSString
        + dataGroupMemberWaitLeaderMS + ", "
        + dataGroupMemberWaitLeaderCounter + ", "
        + (double) dataGroupMemberWaitLeaderMS.get() / dataGroupMemberWaitLeaderCounter.get()
        + "\n";
    result += metaGroupMemberExecuteNonQueryMSString
        + metaGroupMemberExecuteNonQueryMS + ", "
        + metaGroupMemberExecuteNonQueryCounter + ", "
        + (double) metaGroupMemberExecuteNonQueryMS.get() / metaGroupMemberExecuteNonQueryCounter
        .get() + "\n";
    result += metaGroupMemberExecuteNonQueryInLocalGroupMSString
        + metaGroupMemberExecuteNonQueryInLocalGroupMS + ", "
        + metaGroupMemberExecuteNonQueryInLocalGroupCounter + ", "
        + (double) metaGroupMemberExecuteNonQueryInLocalGroupMS.get()
        / metaGroupMemberExecuteNonQueryInLocalGroupCounter.get() + "\n";
    result += metaGroupMemberExecuteNonQueryInRemoteGroupMSString
        + metaGroupMemberExecuteNonQueryInRemoteGroupMS + ", "
        + metaGroupMemberExecuteNonQueryInRemoteGroupCounter + ", "
        + (double) metaGroupMemberExecuteNonQueryInRemoteGroupMS.get()
        / metaGroupMemberExecuteNonQueryInRemoteGroupCounter.get() + "\n";
    result += raftMemberAppendLogMSString
        + raftMemberAppendLogMS + ", "
        + raftMemberAppendLogCounter + ", "
        + (double) raftMemberAppendLogMS.get() / raftMemberAppendLogCounter.get() + "\n";
    result += raftMemberSendLogToFollowerMSString
        + raftMemberSendLogToFollowerMS + ", "
        + raftMemberSendLogToFollowerCounter + ", "
        + (double) raftMemberSendLogToFollowerMS.get() / raftMemberSendLogToFollowerCounter.get()
        + "\n";
    result += raftMemberCommitLogMSString
        + raftMemberCommitLogMS + ", "
        + raftMemberCommitLogCounter + ", "
        + (double) raftMemberCommitLogMS.get() / raftMemberCommitLogCounter.get() + "\n";
    result += raftFollowerAppendEntryString
        + raftFollowerAppendEntryMS + ", "
        + raftFollowerAppendEntryCounter + ", "
        + (double) raftFollowerAppendEntryMS.get() / raftFollowerAppendEntryCounter.get() + "\n";

    return result;
  }
}
