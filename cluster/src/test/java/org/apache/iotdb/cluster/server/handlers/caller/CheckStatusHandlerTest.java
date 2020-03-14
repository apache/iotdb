package org.apache.iotdb.cluster.server.handlers.caller;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.junit.Before;
import org.junit.Test;

public class CheckStatusHandlerTest {
  private MetaGroupMember metaGroupMember;
  private boolean catchUpFlag;

  @Before
  public void setUp() {
    metaGroupMember = new TestMetaGroupMember() {
      @Override
      public void catchUp(Node follower, long followerLastLogIndex) {
        synchronized (metaGroupMember) {
          catchUpFlag = true;
          metaGroupMember.notifyAll();
        }
      }

      @Override
      public LogManager getLogManager() {
        return new TestLogManager();
      }
    };
  }

  @Test
  public void testComplete() throws InterruptedException {
    CheckStatusHandler checkStatusHandler = new CheckStatusHandler();
    // TODO
  }

}
