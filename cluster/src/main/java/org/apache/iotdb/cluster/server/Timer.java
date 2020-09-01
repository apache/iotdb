package org.apache.iotdb.cluster.server;

public class Timer {

  public static long dataGroupMemberProcessPlanLocallyMS = 0L;
  public static long dataGroupMemberProcessPlanLocallyCounter = 0L;
  public static long dataGroupMemberWaitLeaderMS = 0L;
  public static long dataGroupMemberWaitLeaderCounter = 0L;
  public static long metaGroupMemberExecuteNonQueryMS = 0L;
  public static long metaGroupMemberExecuteNonQueryCounter = 0L;
  public static long metaGroupMemberExecuteNonQueryInLocalGroupMS = 0L;
  public static long metaGroupMemberExecuteNonQueryInLocalGroupCounter = 0L;
  public static long metaGroupMemberExecuteNonQueryInRemoteGroupMS = 0L;
  public static long metaGroupMemberExecuteNonQueryInRemoteGroupCounter = 0L;
  public static long raftMemberAppendLogMS = 0L;
  public static long raftMemberAppendLogCounter = 0L;
  public static long raftMemberSendLogToFollowerMS = 0L;
  public static long raftMemberSendLogToFollowerCounter = 0L;
  public static long raftMemberCommitLogMS = 0L;
  public static long raftMemberCommitLogCounter = 0L;

  private static final String dataGroupMemberProcessPlanLocallyMSString = "Data group member - process plan locally : ";
  private static final String dataGroupMemberWaitLeaderMSString = "Data group member - wait leader: ";
  private static final String metaGroupMemberExecuteNonQueryMSString = "Meta group member - execute non query: ";
  private static final String metaGroupMemberExecuteNonQueryInLocalGroupMSString = "Meta group member - execute in local group: ";
  private static final String metaGroupMemberExecuteNonQueryInRemoteGroupMSString = "Meta group member - execute in remote group: ";
  private static final String raftMemberAppendLogMSString = "Raft member - append log: ";
  private static final String raftMemberSendLogToFollowerMSString = "Raft member - send log to follower: ";
  private static final String raftMemberCommitLogMSString = "Raft member - commit log: ";

  public static String getReport() {
    String result = "";
    result += dataGroupMemberProcessPlanLocallyMSString
        + dataGroupMemberProcessPlanLocallyMS + ", "
        + dataGroupMemberProcessPlanLocallyCounter + ", "
        + (double) dataGroupMemberProcessPlanLocallyMS / dataGroupMemberProcessPlanLocallyCounter
        + "\n";
    result += dataGroupMemberWaitLeaderMSString
        + dataGroupMemberWaitLeaderMS + ", "
        + dataGroupMemberWaitLeaderCounter + ", "
        + (double) dataGroupMemberWaitLeaderMS / dataGroupMemberWaitLeaderCounter + "\n";
    result += metaGroupMemberExecuteNonQueryMSString
        + metaGroupMemberExecuteNonQueryMS + ", "
        + metaGroupMemberExecuteNonQueryCounter + ", "
        + (double) metaGroupMemberExecuteNonQueryMS / metaGroupMemberExecuteNonQueryCounter + "\n";
    result += metaGroupMemberExecuteNonQueryInLocalGroupMSString
        + metaGroupMemberExecuteNonQueryInLocalGroupMS + ", "
        + metaGroupMemberExecuteNonQueryInLocalGroupCounter + ", "
        + (double) metaGroupMemberExecuteNonQueryInLocalGroupMS
        / metaGroupMemberExecuteNonQueryInLocalGroupCounter + "\n";
    result += metaGroupMemberExecuteNonQueryInRemoteGroupMSString
        + metaGroupMemberExecuteNonQueryInRemoteGroupMS + ", "
        + metaGroupMemberExecuteNonQueryInRemoteGroupCounter + ", "
        + (double) metaGroupMemberExecuteNonQueryInRemoteGroupMS
        / metaGroupMemberExecuteNonQueryInRemoteGroupCounter + "\n";
    result += raftMemberAppendLogMSString
        + raftMemberAppendLogMS + ", "
        + raftMemberAppendLogCounter + ", "
        + (double) raftMemberAppendLogMS / raftMemberAppendLogCounter + "\n";
    result += raftMemberSendLogToFollowerMSString
        + raftMemberSendLogToFollowerMS + ", "
        + raftMemberSendLogToFollowerCounter + ", "
        + (double) raftMemberSendLogToFollowerMS / raftMemberSendLogToFollowerCounter + "\n";
    result += raftMemberCommitLogMSString
        + raftMemberCommitLogMS + ", "
        + raftMemberCommitLogCounter + ", "
        + (double) raftMemberCommitLogMS / raftMemberCommitLogCounter + "\n";

    return result;
  }
}
