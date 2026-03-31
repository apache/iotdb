package com.timecho.iotdb.ainode.it;

import org.apache.iotdb.ainode.utils.AINodeTestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.AIClusterIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.ainode.it.AINodeCallInferenceIT.callInferenceTest;
import static org.apache.iotdb.ainode.it.AINodeForecastIT.forecastTableFunctionTest;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTree;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class TimechoAINodeModelManagementIT {

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTree();
    prepareDataInTable();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void userDefinedBuiltinModelManagementTestInTree()
      throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      AINodeTestUtils.FakeModelInfo modelInfo =
          new AINodeTestUtils.FakeModelInfo("new_sundial_tree", "sundial", "builtin", "active");
      registerUserDefinedModel(
          statement,
          "new_sundial_tree",
          "sundial",
          "sundial",
          "file:///data/ainode/models/sundial/");
      callInferenceTest(statement, modelInfo);
      dropUserDefinedModel(statement, modelInfo.getModelId());
    }
  }

  @Test
  public void userDefinedModelManagementTestInTable() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      AINodeTestUtils.FakeModelInfo modelInfo =
          new AINodeTestUtils.FakeModelInfo("new_sundial_table", "sundial", "builtin", "active");
      registerUserDefinedModel(
          statement,
          "new_sundial_table",
          "sundial",
          "sundial",
          "file:///data/ainode/models/sundial/");
      forecastTableFunctionTest(statement, modelInfo);
      dropUserDefinedModel(statement, modelInfo.getModelId());
    }
  }

  public static void registerUserDefinedModel(
      Statement statement,
      String newModelName,
      String originModelName,
      String originType,
      String uri)
      throws SQLException, InterruptedException {
    final String CREATE_MODEL_TEMPLATE = "CREATE MODEL %s FROM MODEL %s using uri \"%s\"";
    final String alterConfigSQL = "set configuration \"trusted_uri_pattern\"='.*'";
    final String registerSql =
        String.format(CREATE_MODEL_TEMPLATE, newModelName, originModelName, uri);
    final String showSql = String.format("SHOW MODELS %s", newModelName);
    statement.execute(alterConfigSQL);
    statement.execute(registerSql);
    boolean loading = true;
    for (int retryCnt = 0; retryCnt < 100; retryCnt++) {
      try (ResultSet resultSet = statement.executeQuery(showSql)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
        while (resultSet.next()) {
          String resultModelId = resultSet.getString(1);
          String resultModelType = resultSet.getString(2);
          String resultCategory = resultSet.getString(3);
          String state = resultSet.getString(4);
          assertEquals(newModelName, resultModelId);
          assertEquals(originType, resultModelType);
          assertEquals("user_defined", resultCategory);
          if (state.equals("active")) {
            loading = false;
          } else if (state.equals("loading")) {
            break;
          } else {
            fail("Unexpected status of model: " + state);
          }
        }
      }
      if (!loading) {
        break; // Model is loaded successfully
      }
      TimeUnit.SECONDS.sleep(1);
    }
    assertFalse(loading);
  }

  public static void dropUserDefinedModel(Statement statement, String modelId) throws SQLException {
    final String showSql = String.format("SHOW MODELS %s", modelId);
    final String dropSql = String.format("DROP MODEL %s", modelId);
    statement.execute(dropSql);
    try (ResultSet resultSet = statement.executeQuery(showSql)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(0, count);
    }
  }
}
