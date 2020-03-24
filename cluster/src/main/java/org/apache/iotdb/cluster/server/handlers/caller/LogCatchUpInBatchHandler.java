package org.apache.iotdb.cluster.server.handlers.caller;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;
import static org.apache.iotdb.cluster.server.Response.RESPONSE_LOG_MISMATCH;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogCatchUpInBatchHandler implements AsyncMethodCallback<Long> {

  private static final Logger logger = LoggerFactory.getLogger(LogCatchUpInBatchHandler.class);

  private Node follower;
  private List<ByteBuffer> logs;
  private AtomicBoolean appendSucceed;
  private String memberName;
  private RaftMember raftMember;

  @Override
  public void onComplete(Long response) {
    logger.debug("{}: Received a catch-up result size of {} from {}", memberName, logs.size(),
        follower);
    long resp = response;
    if (resp == RESPONSE_AGREE) {
      synchronized (appendSucceed) {
        appendSucceed.set(true);
        appendSucceed.notifyAll();
      }
      logger.debug("{}: Succeeded to send logs, size is {}", memberName, logs.size());
    } else if (resp == RESPONSE_LOG_MISMATCH) {
      // this is not probably possible
      logger.error("{}: Log mismatch occurred when sending log, which size is {}", memberName,
          logs.size());
      synchronized (appendSucceed) {
        appendSucceed.notifyAll();
      }
    } else {
      // the follower's term has updated, which means a new leader is elected
      synchronized (raftMember.getTerm()) {
        long currTerm = raftMember.getTerm().get();
        if (currTerm < resp) {
          logger.debug("{}: Received a rejection because term is stale: {}/{}", memberName,
              currTerm, resp);
          raftMember.retireFromLeader(resp);
        }
      }
      synchronized (appendSucceed) {
        appendSucceed.notifyAll();
      }
      logger.warn("{}: Catch-up aborted because leadership is lost", memberName);
    }
  }

  @Override
  public void onError(Exception exception) {
    synchronized (appendSucceed) {
      appendSucceed.notifyAll();
    }
    logger.warn("{}: Catch-up fails when sending log, which size is {}", memberName, logs.size(),
        exception);
  }


  public void setAppendSucceed(AtomicBoolean appendSucceed) {
    this.appendSucceed = appendSucceed;
  }

  public void setRaftMember(RaftMember raftMember) {
    this.raftMember = raftMember;
    this.memberName = raftMember.getName();
  }

  public void setFollower(Node follower) {
    this.follower = follower;
  }

  public void setLogs(List<ByteBuffer> logs) {
    this.logs = logs;
  }
}
