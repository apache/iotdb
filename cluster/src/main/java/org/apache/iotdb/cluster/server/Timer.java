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
}
