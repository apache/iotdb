package org.apache.iotdb.db.it.auth;

import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.PrivilegeLevel;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringJoiner;

import static org.apache.iotdb.db.it.IoTDBSetConfigurationIT.checkConfigFileContains;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IOTDBIPCheckIT {
  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableAuditLog(true)
        .setAuditableOperationType(
            new StringJoiner(",")
                .add(AuditLogOperation.DDL.toString())
                .add(AuditLogOperation.DML.toString())
                .add(AuditLogOperation.QUERY.toString())
                .add(AuditLogOperation.CONTROL.toString())
                .toString())
        .setAuditableOperationLevel(PrivilegeLevel.GLOBAL.toString())
        .setAuditableOperationResult("SUCCESS,FAIL");
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSetWhiteListWithExactMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute(
          "set configuration enable_white_list='true',enable_black_list='false', white_ip_list='127.0.0.1' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));

      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=false", "enable_black_list=false")));
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "white_ip_list=127.0.0.1", "white_ip_list=127.0.0.1")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetWhiteListWithFuzzyMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute(
          "set configuration enable_white_list='true',enable_black_list='false',white_ip_list='127.0.*.*' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));

      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=false", "enable_black_list=false")));
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "white_ip_list=127.0.*.*", "white_ip_list=127.0.*.*")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetWhiteListWithNull() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute(
          "set configuration enable_white_list='true',enable_black_list='false',white_ip_list='' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));

      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=false", "enable_black_list=false")));
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(nodeWrapper, "white_ip_list=", "white_ip_list=")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetBlackListWithExactMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_black_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      adminStmt.execute("set configuration enable_white_list='false' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=false", "enable_white_list=false")));
      adminStmt.execute("set configuration black_ip_list='127.0.0.1' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "black_ip_list=127.0.0.1", "black_ip_list=127.0.0.1")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetBlackListWithFuzzyMatch() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_black_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      adminStmt.execute("set configuration enable_white_list='false' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=false", "enable_white_list=false")));
      adminStmt.execute("set configuration black_ip_list='127.0.*.*' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "black_ip_list=127.0.*.*", "black_ip_list=127.0.*.*")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });

    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetBlackListWithNull() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute("set configuration enable_black_list='true' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      adminStmt.execute("set configuration enable_white_list='false' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=false", "enable_white_list=false")));
      adminStmt.execute("set configuration black_ip_list='' ");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(nodeWrapper, "black_ip_list=", "black_ip_list=")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetIPBothInBlackAndWhiteList() {
    try {
      Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
      Statement adminStmt = adminCon.createStatement();
      adminStmt.execute(
          "set configuration enable_black_list='true',black_ip_list='127.0.0.1',white_ip_list='127.0.0.1',enable_white_list='true'");
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_black_list=true", "enable_black_list=true")));
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "black_ip_list=127.0.0.1", "black_ip_list=127.0.0.1")));
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "white_ip_list=127.0.0.1", "white_ip_list=127.0.0.1")));
      Assert.assertTrue(
          EnvFactory.getEnv().getConfigNodeWrapperList().stream()
              .allMatch(
                  nodeWrapper ->
                      checkConfigFileContains(
                          nodeWrapper, "enable_white_list=true", "enable_white_list=true")));
      adminCon.close();
      Assert.assertTrue(adminCon.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          });
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testBlacklistRejectionAuditLog() throws Exception {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("SET CONFIGURATION enable_black_list='true', black_ip_list='127.0.0.1'");
    }

    Assert.assertThrows(
        SQLException.class, () -> EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT));

    // wait for audit log to flush before shutting down (can't poll — IP is blocked)
    Thread.sleep(10_000);

    restartDataNodesWithConfigModification("enable_black_list=true", "enable_black_list=false");

    waitForAuditLogEntry("LOGIN_REJECT_IP", 60_000L);
  }

  @Test
  public void testWhitelistRejectionAuditLog() throws Exception {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("SET CONFIGURATION enable_white_list='true', white_ip_list='10.0.0.1'");
    }

    Assert.assertThrows(
        SQLException.class, () -> EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT));

    // wait for audit log to flush before shutting down (can't poll — IP is blocked)
    Thread.sleep(10_000);

    restartDataNodesWithConfigModification("enable_white_list=true", "enable_white_list=false");

    waitForAuditLogEntry("LOGIN_REJECT_IP", 60_000L);
  }

  @Test
  public void testBothBlacklistAndWhitelistRejectionAuditLog() throws Exception {
    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute(
          "SET CONFIGURATION enable_black_list='true', black_ip_list='127.0.0.1',"
              + " enable_white_list='true', white_ip_list='127.0.0.1'");
    }

    Assert.assertThrows(
        SQLException.class, () -> EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT));

    // wait for audit log to flush before shutting down (can't poll — IP is blocked)
    Thread.sleep(10_000);

    restartDataNodesWithConfigModification("enable_black_list=true", "enable_black_list=false");
    restartDataNodesWithConfigModification("enable_white_list=true", "enable_white_list=false");

    waitForAuditLogEntry("LOGIN_REJECT_IP", 60_000L);
  }

  private void waitForAuditLogEntry(String expectedEventType, long timeoutMs) throws Exception {
    long start = System.currentTimeMillis();
    while (true) {
      try (Connection verifyCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement verifyStmt = verifyCon.createStatement()) {
        ResultSet rs = verifyStmt.executeQuery("SELECT * FROM __audit.audit_log ORDER BY TIME");
        while (rs.next()) {
          String eventType = rs.getString("audit_event_type");
          if (expectedEventType.equals(eventType)) {
            Assert.assertFalse(rs.getBoolean("result"));
            return;
          }
        }
      } catch (Exception e) {
        // connection may not be ready yet
      }
      if (System.currentTimeMillis() - start > timeoutMs) {
        Assert.fail(expectedEventType + " audit log not found within timeout");
      }
      Thread.sleep(1000);
    }
  }

  private void restartDataNodesWithConfigModification(String oldStr, String newStr)
      throws IOException {
    EnvFactory.getEnv().shutdownAllDataNodes();
    for (AbstractNodeWrapper dnw :
        EnvFactory.getEnv().getDataNodeWrapperList().toArray(new AbstractNodeWrapper[0])) {
      modifyConfigFile(dnw, oldStr, newStr);
    }
    for (AbstractNodeWrapper cnw :
        EnvFactory.getEnv().getConfigNodeWrapperList().toArray(new AbstractNodeWrapper[0])) {
      modifyConfigFile(cnw, oldStr, newStr);
    }
    EnvFactory.getEnv().startAllDataNodes();
    waitForConnection(60_000L);
  }

  private void modifyConfigFile(AbstractNodeWrapper wrapper, String oldStr, String newStr)
      throws IOException {
    String path =
        wrapper.getNodePath()
            + File.separator
            + "conf"
            + File.separator
            + "iotdb-system.properties";
    File f = new File(path);
    if (!f.exists()) {
      return;
    }
    String content = new String(Files.readAllBytes(f.toPath()));
    content = content.replace(oldStr, newStr);
    Files.write(f.toPath(), content.getBytes());
  }

  private void waitForConnection(long timeoutMs) {
    long start = System.currentTimeMillis();
    while (true) {
      try (Connection c = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
        break;
      } catch (Exception e) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          break;
        }
      }
      if (System.currentTimeMillis() - start > timeoutMs) {
        Assert.fail("Timeout waiting for DataNode to restart");
      }
    }
  }
}
