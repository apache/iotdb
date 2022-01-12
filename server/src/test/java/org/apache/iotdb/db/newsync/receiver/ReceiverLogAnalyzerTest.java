package org.apache.iotdb.db.newsync.receiver;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.newsync.receiver.manager.PipeInfo;
import org.apache.iotdb.db.newsync.receiver.manager.PipeStatus;
import org.apache.iotdb.db.newsync.receiver.recover.ReceiverLog;
import org.apache.iotdb.db.newsync.receiver.recover.ReceiverLogAnalyzer;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/** This test is for ReceiverLog and ReceiverLogAnalyzer */
public class ReceiverLogAnalyzerTest {

  private static final String pipe1 = "pipe1";
  private static final String pipe2 = "pipe2";
  private static final String ip1 = "192.168.1.11";
  private static final String ip2 = "192.168.2.22";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() {
    try {
      ReceiverLog log = new ReceiverLog();
      log.createPipe(pipe1, ip1, 1);
      log.createPipe(pipe2, ip2, 2);
      log.createPipe(pipe1, ip2, 3);
      log.pausePipe(pipe1, ip1);
      log.pausePipe(pipe2, ip2);
      log.dropPipe(pipe1, ip2);
      log.startPipe(pipe1, ip1);
      Map<String, Map<String, PipeInfo>> map = ReceiverLogAnalyzer.recover();
      Assert.assertNotNull(map);
      Assert.assertEquals(2, map.get(pipe1).size());
      Assert.assertEquals(1, map.get(pipe2).size());
      Assert.assertEquals(1, map.get(pipe2).size());
      Assert.assertEquals(new PipeInfo(pipe2, ip2, PipeStatus.PAUSE, 2), map.get(pipe2).get(ip2));
      Assert.assertEquals(new PipeInfo(pipe1, ip1, PipeStatus.RUNNING, 1), map.get(pipe1).get(ip1));
      Assert.assertEquals(new PipeInfo(pipe1, ip2, PipeStatus.DROP, 3), map.get(pipe1).get(ip2));
    } catch (Exception e) {
      Assert.fail();
      e.printStackTrace();
    }
  }
}
