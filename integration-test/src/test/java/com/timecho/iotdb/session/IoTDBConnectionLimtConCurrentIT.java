package com.timecho.iotdb.session;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.runtime.NodeConnection;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConnectionLimtConCurrentIT {

  private static final AtomicInteger totalSuccess = new AtomicInteger(0);
  private static final AtomicInteger totalFail = new AtomicInteger(0);
  private static final List<Connection> HOLD = Collections.synchronizedList(new ArrayList<>());

  @BeforeClass
  public static void setUp() throws Exception {
    System.setProperty("ReadAndVerifyWithMultiNode", "false");
    EnvFactory.getEnv().getConfig().getCommonConfig().setDnRpcMaxConcurrentClientNum(100);
    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testConCurrentConnection() throws InterruptedException, SQLException {

    String username3 = "user3";
    String password3 = "User3@1234567890";
    String username2 = "user2";
    String password2 = "User2@1234567890";
    Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
    Statement adminStmt = adminCon.createStatement();
    adminStmt.execute(String.format("create user %s '%s'", username3, password3));
    adminStmt.execute(String.format("create user %s '%s'", username2, password2));
    adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 96");
    adminCon.close();

    formSessionConcurrent("user3", "User3@1234567890", 9);
    int success = totalSuccess.get();
    int fail = totalFail.get();
    Assert.assertEquals(3, success);
    Assert.assertEquals(6, fail);
    Assert.assertEquals(3, HOLD.size());
    for (Connection conn : HOLD) {
      conn.close();
    }
  }

  public static void formSessionConcurrent(String username, String password, int concurrency)
      throws InterruptedException {

    CyclicBarrier barrier = new CyclicBarrier(concurrency);
    Thread[] threads = new Thread[concurrency];

    for (int i = 0; i < concurrency; i++) {
      threads[i] =
          new Thread(
              () -> {
                try {
                  barrier.await();
                } catch (Exception e) {
                  fail(e.getMessage());
                }
                try {
                  NodeConnection nodeConn =
                      EnvFactory.getEnv()
                          .getWriteConnection(null, username, password, BaseEnv.TREE_SQL_DIALECT);
                  Connection connection = nodeConn.getUnderlyingConnection();
                  Statement user3Stmt = connection.createStatement();
                  user3Stmt.execute("list user");
                  HOLD.add(connection);
                  totalSuccess.incrementAndGet();
                } catch (SQLException e) {
                  totalFail.incrementAndGet();
                }
              },
              "conn-" + i);
      threads[i].start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }
}
