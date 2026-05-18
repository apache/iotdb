package com.timecho.iotdb.ainode.it;

import org.apache.iotdb.ainode.utils.AINodeTestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.AIClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.ainode.it.AINodeSharedClusterIT.registerUserDefinedModel;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable2;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeClassifyIT {
  private static final String CLASSIFY_TABLE_FUNCTION_SQL_TEMPLATE =
      "SELECT * FROM CLASSIFY("
          + "model_id=>'%s', "
          + "inputs=>(SELECT time, %s FROM db.AI2 WHERE time<%d ORDER BY time DESC LIMIT %d) ORDER BY time, "
          + "timecol=>'%s'"
          + ")";

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTable2();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void classifyTableFunctionTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      classifyTableFunctionTest(
          statement,
          new AINodeTestUtils.FakeModelInfo(
              "user_mantis", "custom_mantis", "user_defined", "active"));
    }
  }

  public static void classifyTableFunctionTest(
      Statement statement, AINodeTestUtils.FakeModelInfo modelInfo)
      throws SQLException, InterruptedException {
    // Register user_mantis first
    registerUserDefinedModel(statement, modelInfo, "file:///data/mantis");

    // Invoke classify table function for specified models and check the validity of results.
    String classifyTableFunctionSQL =
        String.format(
            CLASSIFY_TABLE_FUNCTION_SQL_TEMPLATE,
            modelInfo.getModelId(),
            "s0,s1,s2,s3,s4,s5,s6,s7,s8,s9",
            2880,
            2880,
            "time");
    try (ResultSet resultSet = statement.executeQuery(classifyTableFunctionSQL)) {
      // Check the header of results.
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "category");
      // Check the count of results.
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      Assert.assertEquals(1, count);
    }
  }
}
