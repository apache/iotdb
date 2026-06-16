package com.timecho.iotdb.session;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.auth.IoTDBAuthIT.validateResultSet;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConnectionNumLimitIT {

  @Test
  public void testTotalConnectionPool() throws SQLException {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDnRpcMaxConcurrentClientNum(2);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    Connection adminCon = null;
    try {
      adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);

      SQLException exception =
          Assert.assertThrows(
              SQLException.class,
              () -> EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT));
      Assert.assertTrue(
          exception.getMessage(),
          exception.getMessage().contains("There are not enough connection resources available"));
    } finally {
      if (adminCon != null) {
        adminCon.close();
      }
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }

  @Test
  public void testCliConnectionResourceLimitError() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDnRpcMaxConcurrentClientNum(2);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    Connection adminCon = null;
    try {
      adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);

      CliResult cliResult = executeCliCommand("show cluster");
      Assert.assertEquals(cliResult.output, 1, cliResult.exitCode);
      Assert.assertTrue(
          cliResult.output, cliResult.output.contains("Error: Can't execute sql because"));
      Assert.assertTrue(
          cliResult.output,
          cliResult.output.contains("There are not enough connection resources available"));
      Assert.assertTrue(
          cliResult.output,
          cliResult.output.contains("Connection reset")
              || cliResult.output.contains("Socket is closed by peer"));
    } finally {
      if (adminCon != null) {
        adminCon.close();
      }
      EnvFactory.getEnv().cleanClusterEnvironment();
    }
  }

  @Test
  public void testSessionLimitPerUserTree() throws SQLException {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDnRpcMaxConcurrentClientNum(12);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      // init users.
      adminStmt.execute("create user user3 'user31234567890D@'");
      adminStmt.execute("create user user2 'user21234567890D@'");
      adminStmt.execute("create user user1 'user11234567890D@'");

      // 1. test maxSessionPerUser
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 0");
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
      Connection user3_1 =
          EnvFactory.getEnv().getConnection("user3", "user31234567890D@", BaseEnv.TREE_SQL_DIALECT);
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 1");
      Assert.assertTrue(!user3_1.isClosed()); //
      SQLException maxSessionExceededException =
          Assert.assertThrows(
              SQLException.class,
              () -> {
                EnvFactory.getEnv()
                    .getConnection("user3", "user31234567890D@", BaseEnv.TREE_SQL_DIALECT);
              });
      Assert.assertTrue(
          maxSessionExceededException.getMessage(),
          maxSessionExceededException
              .getMessage()
              .contains(
                  "The current number of connections for user user3 has reached its maximum limit. Please try again later."));
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 2");
      Assert.assertTrue(!user3_1.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user3", "user31234567890D@", BaseEnv.TREE_SQL_DIALECT);
          });
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 3");
      // two logins: one login success and one login fails.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user3", "user31234567890D@", BaseEnv.TREE_SQL_DIALECT);
          });
      // two idle connections left.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 5");
      Connection user3_2 =
          EnvFactory.getEnv().getConnection("user3", "user31234567890D@", BaseEnv.TREE_SQL_DIALECT);
      user3_1.close();
      Assert.assertTrue(user3_1.isClosed());
      user3_2.close();
      Assert.assertTrue(user3_2.isClosed());
      // recovery.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");

      // 2. test minSessionPerUser
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 12");
          });

      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 10");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user2", "user21234567890D@", BaseEnv.TREE_SQL_DIALECT);
          });
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 8");
      Connection user2Conn =
          EnvFactory.getEnv().getConnection("user2", "user21234567890D@", BaseEnv.TREE_SQL_DIALECT);
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER 1");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 3");
          });
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 2");
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 6");
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 3");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER 1");

      // recovery
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");

      // 3. parameter validation
      // The maximum and minimum number of connections cannot exceed the value of
      // dn_rpc_max_comput_cient_num.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 16");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 16");
          });
      // The minimum number of connections cannot exceed the maximum number of connections.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 6");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 5");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 8");
          });
      // Negative numbers less than -1 are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -2");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -3");
          });
      // Decimals are not allowed
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 2.2");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 3.3");
          });
      // Characters are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER a");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER b");
          });
      // Fractions are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 1/9");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 9/3");
          });

      // revovery
      // recovery
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");

      // 4. permission check
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer root SET MAX_SESSION_PER_USER -1");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer root SET MIN_SESSION_PER_USER -1");
          });
      Connection user3Conn =
          EnvFactory.getEnv().getConnection("user3", "user31234567890D@", BaseEnv.TREE_SQL_DIALECT);
      Statement user3Stmt = user3Conn.createStatement();
      Assert.assertThrows(
          SQLException.class,
          () -> {
            user3Stmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            user3Stmt.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
          });
      // test "set dn_rpc_max_concurrent_client_num"
      adminStmt.execute("set configuration 'dn_rpc_max_concurrent_client_num'='1000'");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            user3Stmt.execute("set configuration 'dn_rpc_max_concurrent_client_num'='1000'");
          });
      user3Conn.close();
    }

    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSessionLimitPerUserTable() throws SQLException {
    EnvFactory.getEnv().getConfig().getCommonConfig().setDnRpcMaxConcurrentClientNum(12);

    // Init 1C1D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);

    try (Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement adminStmt = adminCon.createStatement()) {
      // init users.
      adminStmt.execute("create user user3 'user31234567890D@'");
      adminStmt.execute("create user user2 'user21234567890D@'");
      adminStmt.execute("create user user1 'user11234567890D@'");

      // 1. test maxSessionPerUser
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 0");
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
      Connection user3_1 =
          EnvFactory.getEnv()
              .getConnection("user3", "user31234567890D@", BaseEnv.TABLE_SQL_DIALECT);
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 1");
      Assert.assertTrue(!user3_1.isClosed()); //
      SQLException maxSessionExceededException =
          Assert.assertThrows(
              SQLException.class,
              () -> {
                EnvFactory.getEnv()
                    .getConnection("user3", "user31234567890D@", BaseEnv.TABLE_SQL_DIALECT);
              });
      Assert.assertTrue(
          maxSessionExceededException.getMessage(),
          maxSessionExceededException
              .getMessage()
              .contains(
                  "The current number of connections for user user3 has reached its maximum limit. Please try again later."));
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 2");
      Assert.assertTrue(!user3_1.isClosed());
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user3", "user31234567890D@", BaseEnv.TABLE_SQL_DIALECT);
          });
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 3");
      // two logins: one login success and one login fails.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user3", "user31234567890D@", BaseEnv.TABLE_SQL_DIALECT);
          });
      // two idle connections left.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 5");
      Connection user3_2 =
          EnvFactory.getEnv()
              .getConnection("user3", "user31234567890D@", BaseEnv.TABLE_SQL_DIALECT);
      user3_1.close();
      Assert.assertTrue(user3_1.isClosed());
      user3_2.close();
      Assert.assertTrue(user3_2.isClosed());
      // recovery.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");

      // 2. test minSessionPerUser
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 12");
          });

      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 10");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            EnvFactory.getEnv()
                .getConnection("user2", "user21234567890D@", BaseEnv.TABLE_SQL_DIALECT);
          });
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 8");
      Connection user2Conn =
          EnvFactory.getEnv()
              .getConnection("user2", "user21234567890D@", BaseEnv.TABLE_SQL_DIALECT);
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER 1");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 3");
          });
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 2");
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 6");
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER 3");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER 1");

      // recovery
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");

      // 3. parameter validation
      // The maximum and minimum number of connections cannot exceed the value of
      // dn_rpc_max_comput_cient_num.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 16");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 16");
          });
      // The minimum number of connections cannot exceed the maximum number of connections.
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 6");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 5");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 8");
          });
      // Negative numbers less than -1 are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -2");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -3");
          });
      // Decimals are not allowed
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 2.2");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 3.3");
          });
      // Characters are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER a");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER b");
          });
      // Fractions are not allowed.
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER 1/9");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER 9/3");
          });

      // revovery
      // recovery
      adminStmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");
      adminStmt.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");

      // 4. permission check
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer root SET MAX_SESSION_PER_USER -1");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("ALTER USer root SET MIN_SESSION_PER_USER -1");
          });
      Connection user3Conn =
          EnvFactory.getEnv()
              .getConnection("user3", "user31234567890D@", BaseEnv.TABLE_SQL_DIALECT);
      Statement user3Stmt = user3Conn.createStatement();
      Assert.assertThrows(
          SQLException.class,
          () -> {
            user3Stmt.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
          });
      Assert.assertThrows(
          SQLException.class,
          () -> {
            user3Stmt.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
          });
      // test "set dn_rpc_max_concurrent_client_num"
      adminStmt.execute("set configuration dn_rpc_max_concurrent_client_num='1000'");
      Assert.assertThrows(
          SQLException.class,
          () -> {
            user3Stmt.execute("set configuration dn_rpc_max_concurrent_client_num='1000'");
          });
      user3Conn.close();
    }

    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testStrictSessionLimitPerUserTree() throws SQLException {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    executeNonQuery("set configuration 'enable_separation_of_powers'='true'");

    Assert.assertThrows(
        SQLException.class,
        () -> {
          EnvFactory.getEnv().getConnection("root", "TimechoDB@2021", BaseEnv.TREE_SQL_DIALECT);
        });
    Connection securityConn =
        EnvFactory.getEnv()
            .getConnection("security_admin", "TimechoDB@2021", BaseEnv.TREE_SQL_DIALECT);
    Statement securityStat = securityConn.createStatement();

    Connection auditConn =
        EnvFactory.getEnv()
            .getConnection("audit_admin", "TimechoDB@2021", BaseEnv.TREE_SQL_DIALECT);
    Statement auditStat = auditConn.createStatement();

    Connection sysConn =
        EnvFactory.getEnv().getConnection("sys_admin", "TimechoDB@2021", BaseEnv.TREE_SQL_DIALECT);
    Statement sysStat = sysConn.createStatement();

    // init users.
    securityStat.execute("create user user3 'user31234567890'");
    securityStat.execute("create user user2 'user21234567890'");
    securityStat.execute("create user user1 'user11234567890'");
    securityStat.execute("create user user4 'user41234567890'");
    securityStat.execute("grant security on root.** to user user3");
    auditStat.execute("grant audit on root.** to user user2");
    sysStat.execute("grant system on root.** to user user1");

    Connection user3Conn =
        EnvFactory.getEnv()
            .getConnection("security_admin", "TimechoDB@2021", BaseEnv.TREE_SQL_DIALECT);
    Statement user3Stat = user3Conn.createStatement();

    // test "list user"
    ResultSet resultSet = securityStat.executeQuery("LIST USER");
    String ans =
        "0,root,-1,1,\n"
            + "1,sys_admin,-1,1,\n"
            + "2,security_admin,-1,1,\n"
            + "3,audit_admin,-1,1,\n"
            + "10000,user3,-1,-1,\n"
            + "10001,user2,-1,-1,\n"
            + "10002,user1,-1,-1,\n"
            + "10003,user4,-1,-1,\n";
    validateResultSet(resultSet, ans);
    // test "The number of connections for the built-in admin cannot be modified."
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer root SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer root SET MIN_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer security_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer security_admin SET MIN_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer audit_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer audit_admin SET MIN_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer sys_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer sys_admin SET MIN_SESSION_PER_USER -1");
        });
    // test "security_admin can modify the connection number of other non orginal admin users"
    securityStat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");

    // test "security_admin can modify the connection number of non-admin users"
    securityStat.execute("ALTER USer user4 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user4 SET MIN_SESSION_PER_USER -1");

    // test "users who have security privilege can modify the connection number of other non orginal
    // admin users"
    user3Stat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");
    Assert.assertThrows(
        SQLException.class,
        () -> {
          user3Stat.execute("ALTER USer security_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          user3Stat.execute("ALTER USer audit_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          user3Stat.execute("ALTER USer sys_admin SET MAX_SESSION_PER_USER -1");
        });
    // test "root-audit_admin can not modify connection numbers of itself , non-orginal audit
    // admin,and regular users."
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user4 SET MAX_SESSION_PER_USER -1");
        });
    // test "root-sys_admin can not modify connection numbers of itself , non-orginal audit
    // admin,and regular users."
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user4 SET MAX_SESSION_PER_USER -1");
        });
    // test "set dn_rpc_max_concurrent_client_num"
    securityStat.execute("set configuration 'dn_rpc_max_concurrent_client_num'='1000'");
    user3Stat.execute("set configuration 'dn_rpc_max_concurrent_client_num'='1000'");
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("set 'configuration dn_rpc_max_concurrent_client_num'='1000'");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("set 'configuration dn_rpc_max_concurrent_client_num'='1000'");
        });
    // clear
    securityConn.close();
    auditConn.close();
    sysConn.close();
    user3Conn.close();

    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testStrictSessionLimitPerUserTable() throws SQLException {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    executeNonQuery("set configuration 'enable_separation_of_powers'='true'");

    Assert.assertThrows(
        SQLException.class,
        () -> {
          EnvFactory.getEnv().getConnection("root", "TimechoDB@2021", BaseEnv.TABLE_SQL_DIALECT);
        });
    Connection securityConn =
        EnvFactory.getEnv()
            .getConnection("security_admin", "TimechoDB@2021", BaseEnv.TABLE_SQL_DIALECT);
    Statement securityStat = securityConn.createStatement();

    Connection auditConn =
        EnvFactory.getEnv()
            .getConnection("audit_admin", "TimechoDB@2021", BaseEnv.TABLE_SQL_DIALECT);
    Statement auditStat = auditConn.createStatement();

    Connection sysConn =
        EnvFactory.getEnv().getConnection("sys_admin", "TimechoDB@2021", BaseEnv.TABLE_SQL_DIALECT);
    Statement sysStat = sysConn.createStatement();

    // init users.
    securityStat.execute("create user user3 'user31234567890'");
    securityStat.execute("create user user2 'user21234567890'");
    securityStat.execute("create user user1 'user11234567890'");
    securityStat.execute("create user user4 'user41234567890'");
    securityStat.execute("grant security to user user3");
    auditStat.execute("grant audit to user user2");
    sysStat.execute("grant system to user user1");

    Connection user3Conn =
        EnvFactory.getEnv()
            .getConnection("security_admin", "TimechoDB@2021", BaseEnv.TABLE_SQL_DIALECT);
    Statement user3Stat = user3Conn.createStatement();

    // test "list user"
    ResultSet resultSet = securityStat.executeQuery("LIST USER");
    String ans =
        "0,root,-1,1,\n"
            + "1,sys_admin,-1,1,\n"
            + "2,security_admin,-1,1,\n"
            + "3,audit_admin,-1,1,\n"
            + "10000,user3,-1,-1,\n"
            + "10001,user2,-1,-1,\n"
            + "10002,user1,-1,-1,\n"
            + "10003,user4,-1,-1,\n";
    validateResultSet(resultSet, ans);
    // test "The number of connections for the built-in admin cannot be modified."
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer root SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer root SET MIN_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer security_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer security_admin SET MIN_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer audit_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer audit_admin SET MIN_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer sys_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          securityStat.execute("ALTER USer sys_admin SET MIN_SESSION_PER_USER -1");
        });
    // test "security_admin can modify the connection number of other non orginal admin users"
    securityStat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");

    // test "security_admin can modify the connection number of non-admin users"
    securityStat.execute("ALTER USer user4 SET MAX_SESSION_PER_USER -1");
    securityStat.execute("ALTER USer user4 SET MIN_SESSION_PER_USER -1");

    // test "users who have security privilege can modify the connection number of other non orginal
    // admin users"
    user3Stat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user3 SET MIN_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user2 SET MIN_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
    user3Stat.execute("ALTER USer user1 SET MIN_SESSION_PER_USER -1");
    Assert.assertThrows(
        SQLException.class,
        () -> {
          user3Stat.execute("ALTER USer security_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          user3Stat.execute("ALTER USer audit_admin SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          user3Stat.execute("ALTER USer sys_admin SET MAX_SESSION_PER_USER -1");
        });
    // test "root-audit_admin can not modify connection numbers of itself , non-orginal audit
    // admin,and regular users."
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("ALTER USer user4 SET MAX_SESSION_PER_USER -1");
        });
    // test "root-sys_admin can not modify connection numbers of itself , non-orginal audit
    // admin,and regular users."
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user3 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user2 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user1 SET MAX_SESSION_PER_USER -1");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("ALTER USer user4 SET MAX_SESSION_PER_USER -1");
        });
    // test "set dn_rpc_max_concurrent_client_num"
    securityStat.execute("set configuration dn_rpc_max_concurrent_client_num='1000'");
    user3Stat.execute("set configuration dn_rpc_max_concurrent_client_num='1000'");
    Assert.assertThrows(
        SQLException.class,
        () -> {
          sysStat.execute("set configuration dn_rpc_max_concurrent_client_num='1000'");
        });
    Assert.assertThrows(
        SQLException.class,
        () -> {
          auditStat.execute("set configuration dn_rpc_max_concurrent_client_num='1000'");
        });
    // clear
    securityConn.close();
    auditConn.close();
    sysConn.close();
    user3Conn.close();

    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private CliResult executeCliCommand(String sql) throws IOException, InterruptedException {
    String sbinPath = EnvFactory.getEnv().getSbinPath();
    String libPath = EnvFactory.getEnv().getLibPath();
    String homePath =
        libPath.substring(0, libPath.lastIndexOf(File.separator + "lib" + File.separator + "*"));
    ProcessBuilder builder =
        new ProcessBuilder(
            "bash",
            sbinPath + File.separator + "start-cli.sh",
            "-h",
            EnvFactory.getEnv().getIP(),
            "-p",
            EnvFactory.getEnv().getPort(),
            "-u",
            "root",
            "-pw",
            "TimechoDB@2021",
            "-sql_dialect",
            "table",
            "-e",
            "\"" + sql + "\"");
    builder.environment().put("IOTDB_HOME", homePath);
    builder.redirectErrorStream(true);

    Process process = builder.start();
    List<String> outputLines = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        outputLines.add(line);
      }
    }

    int exitCode = process.waitFor();
    return new CliResult(exitCode, String.join(System.lineSeparator(), outputLines));
  }

  private static class CliResult {
    private final int exitCode;
    private final String output;

    private CliResult(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }
  }
}
