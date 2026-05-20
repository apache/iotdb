package com.timecho.iotdb.ainode.it;

import org.apache.iotdb.ainode.utils.AINodeTestUtils;
import org.apache.iotdb.ainode.utils.AINodeTestUtils.FakeModelInfo;
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
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.ainode.it.AINodeSharedClusterIT.callInferenceTest;
import static org.apache.iotdb.ainode.it.AINodeSharedClusterIT.forecastTableFunctionTest;
import static org.apache.iotdb.ainode.it.AINodeSharedClusterIT.registerUserDefinedModel;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.BUILTIN_MODEL_MAP;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.errorTest;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTable2;
import static org.apache.iotdb.ainode.utils.AINodeTestUtils.prepareDataInTree;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Consolidates AINodeClassifyIT, AINodeCovariateForecastIT, and TimechoAINodeModelManagementIT into
 * a single class that shares one 1C1D1A cluster, avoiding 2 redundant cluster startups. AINode
 * fine-tune tests stay in their own class because they leave persistent fine-tuned models behind.
 */
@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class TimechoAINodeSharedClusterIT {

  private static final String CLASSIFY_TABLE_FUNCTION_SQL_TEMPLATE =
      "SELECT * FROM CLASSIFY("
          + "model_id=>'%s', "
          + "inputs=>(SELECT time, %s FROM db.AI2 WHERE time<%d ORDER BY time DESC LIMIT %d) ORDER BY time, "
          + "timecol=>'%s'"
          + ")";

  private static final String FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE =
      "SELECT * FROM FORECAST("
          + "model_id=>'%s', "
          + "targets=>(SELECT time, %s FROM db.AI WHERE time<%d ORDER BY time DESC LIMIT %d) ORDER BY time, "
          + "history_covs=>'%s',"
          + "future_covs=>'%s',"
          + "output_start_time=>%d, "
          + "output_length=>%d, "
          + "auto_adapt=>%s"
          + ")";
  private static final String HISTORY_COVS_TEMPLATE =
      "(SELECT time, %s FROM db.AI WHERE time < %d ORDER BY time DESC LIMIT %d) ORDER BY time";
  private static final String FUTURE_COVS_TEMPLATE =
      "SELECT time, %s FROM db.AI WHERE time >= %d ORDER BY time LIMIT %d";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareDataInTree();
    prepareDataInTable();
    prepareDataInTable2();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ========== Classify tests (from AINodeClassifyIT) ==========

  @Test
  public void classifyTableFunctionTest() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      FakeModelInfo modelInfo =
          new FakeModelInfo("user_mantis", "custom_mantis", "user_defined", "active");
      registerUserDefinedModel(statement, modelInfo, "file:///data/mantis");

      String classifyTableFunctionSQL =
          String.format(
              CLASSIFY_TABLE_FUNCTION_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1,s2,s3,s4,s5,s6,s7,s8,s9",
              2880,
              2880,
              "time");
      try (ResultSet resultSet = statement.executeQuery(classifyTableFunctionSQL)) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        checkHeader(resultSetMetaData, "category");
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertEquals(1, count);
      }
    }
  }

  // ========== Covariate forecast tests (from AINodeCovariateForecastIT) ==========

  @Test
  public void forecastTableFunctionWithCovariateTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      AINodeTestUtils.FakeModelInfo modelInfo = BUILTIN_MODEL_MAP.get("chronos2");

      String historyCovsSQL = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2880, 2880);
      String futureCovsSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 480);

      String forecastTableFunctionSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              historyCovsSQL,
              futureCovsSQL,
              2880,
              480,
              "false");
      try (ResultSet resultSet = statement.executeQuery(forecastTableFunctionSQL)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertEquals(480, count);
      }

      String historyCovsSQLAutoAdapt = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2881, 2881);
      String futureCovsSQLAutoAdapt = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 479);

      String forecastTableFunctionSQLAutoAdapt =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              historyCovsSQLAutoAdapt,
              futureCovsSQLAutoAdapt,
              2880,
              480,
              "true");
      try (ResultSet resultSet = statement.executeQuery(forecastTableFunctionSQLAutoAdapt)) {
        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        Assert.assertEquals(480, count);
      }
    }
  }

  @Test
  public void forecastTableFunctionWithCovariateErrorTest() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      AINodeTestUtils.FakeModelInfo modelInfo = BUILTIN_MODEL_MAP.get("chronos2");

      String historyCovsSQL = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2880, 2880);
      String futureCovsSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 96);

      // Future_covs_sql is specified yet history_covs_sql is None
      String invalidSyntaxSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              "",
              futureCovsSQL,
              2880,
              96,
              "true");
      errorTest(
          statement,
          invalidSyntaxSQL,
          "1599: Error occurred while executing forecast:[Future_covs_sql is specified yet history_covs_sql is None.]");

      // Invalid history covariates length
      String invalidHistoryCovsLengthSQL =
          String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 2879, 2879);
      String invalidHistoryCovariateLengthSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              invalidHistoryCovsLengthSQL,
              futureCovsSQL,
              2880,
              96,
              "false");
      errorTest(
          statement,
          invalidHistoryCovariateLengthSQL,
          "1599: Error occurred while executing forecast:[Individual `past_covariates` must be 1-d with length equal to the length of `target` (= 2880), found: s2 with shape (2879,) in element at index 0.]");

      // Invalid future covariates length
      String invalidFutureCovsLengthSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 2880, 95);
      String invalidFutureCovariateLengthSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              historyCovsSQL,
              invalidFutureCovsLengthSQL,
              2880,
              96,
              "false");
      errorTest(
          statement,
          invalidFutureCovariateLengthSQL,
          "1599: Error occurred while executing forecast:[Individual `future_covariates` must be 1-d with length equal to `output_length` (= 96), found: s3 with shape (95,) in element at index 0.]");

      // Invalid future covariates set
      String invalidFutureCovsSetSQL = String.format(FUTURE_COVS_TEMPLATE, "s1", 2880, 96);
      String invalidFutureCovariateSetSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              historyCovsSQL,
              invalidFutureCovsSetSQL,
              2880,
              96,
              "false");
      errorTest(
          statement,
          invalidFutureCovariateSetSQL,
          "1599: Error occurred while executing forecast:[Expected keys in `future_covariates` to be a subset of `past_covariates` ['s2', 's3'], but found s1 in element at index 0.]");

      // Empty set - history covariates
      String invalidHistoryCovsEmptySetSQL = String.format(HISTORY_COVS_TEMPLATE, "s2,s3", 0, 96);
      String invalidHistoryCovariateEmptySetSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              invalidHistoryCovsEmptySetSQL,
              futureCovsSQL,
              2880,
              96,
              "true");
      errorTest(
          statement,
          invalidHistoryCovariateEmptySetSQL,
          "1599: Error occurred while executing forecast:[The history covariates are specified but no history data are selected.]");

      // Empty set - future covariates
      String invalidFutureCovsEmptySetSQL = String.format(FUTURE_COVS_TEMPLATE, "s3", 5760, 96);
      String invalidFutureCovariateEmptySetSQL =
          String.format(
              FORECAST_TABLE_FUNCTION_WITH_COVARIATE_SQL_TEMPLATE,
              modelInfo.getModelId(),
              "s0,s1",
              2880,
              2880,
              historyCovsSQL,
              invalidFutureCovsEmptySetSQL,
              2880,
              96,
              "true");
      errorTest(
          statement,
          invalidFutureCovariateEmptySetSQL,
          "1599: Error occurred while executing forecast:[The future covariates are specified but no future data are selected.]");
    }
  }

  // ========== User-defined builtin model management (from TimechoAINodeModelManagementIT)
  // ==========

  @Test
  public void userDefinedBuiltinModelManagementTestInTree()
      throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      FakeModelInfo modelInfo =
          new FakeModelInfo("new_sundial_tree", "sundial", "builtin", "active");
      registerBuiltinAliasModel(
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
      FakeModelInfo modelInfo =
          new FakeModelInfo("new_sundial_table", "sundial", "builtin", "active");
      registerBuiltinAliasModel(
          statement,
          "new_sundial_table",
          "sundial",
          "sundial",
          "file:///data/ainode/models/sundial/");
      forecastTableFunctionTest(statement, modelInfo);
      dropUserDefinedModel(statement, modelInfo.getModelId());
    }
  }

  /**
   * Registers a user-defined model that aliases an existing builtin model via {@code CREATE MODEL X
   * FROM MODEL Y USING URI ...}. Distinct from {@link
   * org.apache.iotdb.ainode.it.AINodeSharedClusterIT#registerUserDefinedModel} which registers a
   * fresh user model.
   */
  private static void registerBuiltinAliasModel(
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
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    assertFalse(loading);
  }

  private static void dropUserDefinedModel(Statement statement, String modelId)
      throws SQLException {
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
