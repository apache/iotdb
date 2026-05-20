package com.timecho.iotdb.ainode.it;

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
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.ainode.utils.AINodeTestUtils.checkHeader;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({AIClusterIT.class})
public class AINodeFineTuneIT {

  // Smoke params: just verify the fine-tune pipeline produces an active model and can serve
  // inference/forecast. Quality is covered by nightly runs, not per-MR CI.
  private static final int LINE_COUNT = 1024;
  private static final int WAITING_COUNT_SECOND = 300;
  private static final String SMOKE_HYPERPARAMETERS =
      "num_train_epochs=1, warmup_steps=10, iter_per_epoch=100, learning_rate=0.00001";
  private static final String[] SCHEMA_SQL_IN_TREE =
      new String[] {
        "CREATE DATABASE root.AI", "CREATE TIMESERIES root.AI.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
      };
  private static final String[] SCHEMA_SQL_IN_TABLE =
      new String[] {
        "CREATE DATABASE root",
        "CREATE TABLE root.AI (s0 FLOAT FIELD, s1 FLOAT FIELD, s2 FLOAT FIELD, s3 FLOAT FIELD, s4 FLOAT FIELD)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    // Init 1C1D1A cluster environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
    prepareFineTuneData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareFineTuneData() {
    String[] write_sql_in_tree = new String[LINE_COUNT + SCHEMA_SQL_IN_TREE.length];
    System.arraycopy(SCHEMA_SQL_IN_TREE, 0, write_sql_in_tree, 0, SCHEMA_SQL_IN_TREE.length);
    for (int i = 0; i < LINE_COUNT; i++) {
      write_sql_in_tree[i + SCHEMA_SQL_IN_TREE.length] =
          String.format("INSERT INTO root.AI(timestamp, s0) VALUES(%d, %f)", i, Math.random());
    }
    prepareData(write_sql_in_tree);
    String[] write_sql_in_table = new String[LINE_COUNT + SCHEMA_SQL_IN_TABLE.length];
    System.arraycopy(SCHEMA_SQL_IN_TABLE, 0, write_sql_in_table, 0, SCHEMA_SQL_IN_TABLE.length);
    for (int i = 0; i < LINE_COUNT; i++) {
      write_sql_in_table[i + SCHEMA_SQL_IN_TABLE.length] =
          String.format(
              "INSERT INTO root.AI(time, s0, s1, s2, s3, s4) VALUES(%d, %f, %f, %f, %f, %f)",
              i, Math.random(), Math.random(), Math.random(), Math.random(), Math.random());
    }
    prepareTableData(write_sql_in_table);
  }

  @Test
  public void testFineTuneInTree() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE MODEL sundial_tree WITH HYPERPARAMETERS ("
              + SMOKE_HYPERPARAMETERS
              + ") FROM MODEL sundial ON DATASET (PATH root.AI.s0)");
      for (int retry = 0; retry < WAITING_COUNT_SECOND; retry++) {
        boolean isActivated = false;
        try (ResultSet resultSet = statement.executeQuery("SHOW MODELS sundial_tree")) {
          int modelCnt = 0;
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
          while (resultSet.next()) {
            String modelId = resultSet.getString(1);
            String modelType = resultSet.getString(2);
            String category = resultSet.getString(3);
            String state = resultSet.getString(4);

            assertEquals("sundial_tree", modelId);
            assertEquals("sundial", modelType);
            assertEquals("fine_tuned", category);
            isActivated = state.equals("active");
            ++modelCnt;
          }
          assertEquals(1, modelCnt);
        }
        if (isActivated) {
          try (ResultSet resultSet =
              statement.executeQuery(
                  "CALL INFERENCE(sundial_tree, \"SELECT s0 FROM root.AI\", outputLength=720)")) {
            int outputCnt = 0;
            while (resultSet.next()) {
              outputCnt++;
            }
            if (720 != outputCnt) {
              fail(
                  "Output count mismatch for fine tuned model."
                      + "Expected: "
                      + 720
                      + ", but got: "
                      + outputCnt);
            }
          }
          return;
        }
        TimeUnit.SECONDS.sleep(1); // Wait for 1 second before retrying
      }
      fail(
          String.format(
              "Fine-tuning model did not finish after waiting for %ds.", WAITING_COUNT_SECOND));
      //      statement.execute("LOAD MODEL sundial_tree TO DEVICES \"0,1\"");
      //      checkModelOnSpecifiedDevice(statement, "sundial_tree", "0,1");
      //      concurrentInference(
      //          statement,
      //          "CALL INFERENCE(sundial_tree, \"SELECT s0 FROM root.AI\", outputLength=720)",
      //          10,
      //          100,
      //          720);
      //      statement.execute("UNLOAD MODEL sundial_tree FROM DEVICES \"0,1\"");
      //      checkModelNotOnSpecifiedDevice(statement, "sundial_tree", "0,1");
    }
  }

  @Test
  public void testFineTuneInTable() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE MODEL sundial_table WITH HYPERPARAMETERS ("
              + SMOKE_HYPERPARAMETERS
              + ") FROM MODEL sundial ON DATASET (\'SELECT time, s0 FROM root.AI\')");
      for (int retry = 0; retry < WAITING_COUNT_SECOND; retry++) {
        boolean isActivated = false;
        try (ResultSet resultSet = statement.executeQuery("SHOW MODELS sundial_table")) {
          int modelCnt = 0;
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
          while (resultSet.next()) {
            String modelId = resultSet.getString(1);
            String modelType = resultSet.getString(2);
            String category = resultSet.getString(3);
            String state = resultSet.getString(4);

            assertEquals("sundial_table", modelId);
            assertEquals("sundial", modelType);
            assertEquals("fine_tuned", category);
            isActivated = state.equals("active");
            ++modelCnt;
          }
          assertEquals(1, modelCnt);
        }
        if (isActivated) {
          final String forecastSQL =
              "SELECT * FROM FORECAST("
                  + "model_id=>'sundial_table', "
                  + "targets=>(SELECT time,s0 FROM root.AI) ORDER BY time, "
                  + "output_length=>720"
                  + ")";
          try (ResultSet resultSet = statement.executeQuery(forecastSQL)) {
            int outputCnt = 0;
            while (resultSet.next()) {
              outputCnt++;
            }
            if (720 != outputCnt) {
              fail(
                  "Output count mismatch for fine tuned model."
                      + "Expected: "
                      + 720
                      + ", but got: "
                      + outputCnt);
            }
            return;
          }
        }
        TimeUnit.SECONDS.sleep(1); // Wait for 1 second before retrying
      }
      fail(
          String.format(
              "Fine-tuning model did not finish after waiting for %ds.", WAITING_COUNT_SECOND));
      // Ensure the fine-tuned model can be employed
      //      statement.execute("LOAD MODEL sundial_table TO DEVICES \"0,1\"");
      //      checkModelOnSpecifiedDevice(statement, "sundial_table", "0,1");
      //      concurrentInference(
      //          statement,
      //          "\"",
      //          10,
      //          100,
      //          720);
      //      statement.execute("UNLOAD MODEL sundial_table FROM DEVICES \"0,1\"");
      //      checkModelNotOnSpecifiedDevice(statement, "sundial_table", "0,1");
    }
  }

  //  @Test
  public void testWeaverCNNFineTune() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE MODEL sundial_weaver_cnn WITH HYPERPARAMETERS (num_train_epochs=2, warmup_steps=80, iter_per_epoch=3000, learning_rate=0.00001, "
              + "finetune_type=weaver_cnn, input_channel=5) FROM MODEL sundial ON DATASET (\'SELECT time, s0, s1, s2, s3, s4 FROM root.AI\')");
      for (int retry = 0; retry < WAITING_COUNT_SECOND; retry++) {
        boolean isActivated = false;
        try (ResultSet resultSet = statement.executeQuery("SHOW MODELS sundial_weaver_cnn")) {
          int modelCnt = 0;
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
          while (resultSet.next()) {
            String modelId = resultSet.getString(1);
            String modelType = resultSet.getString(2);
            String category = resultSet.getString(3);
            String state = resultSet.getString(4);

            assertEquals("sundial_weaver_cnn", modelId);
            assertEquals("sundial", modelType);
            assertEquals("fine_tuned", category);
            isActivated = state.equals("active");
            ++modelCnt;
          }
          assertEquals(1, modelCnt);
        }
        if (isActivated) {
          return;
          // TODO: Enable forecast test
          //          final String forecastSQL =
          //              "SELECT * FROM FORECAST("
          //                  + "model_id=>'sundial_weaver_cnn', "
          //                  + "targets=>(SELECT time,s4 FROM root.AI LIMIT 2880) ORDER BY time, "
          //                  + "history_covs=>\'(SELECT time,s0,s1,s2,s3 FROM root.AI LIMIT 2880)
          // ORDER BY time\', "
          //                  + "output_length=>720"
          //                  + ")";
          //          try (ResultSet resultSet = statement.executeQuery(forecastSQL)) {
          //            int outputCnt = 0;
          //            while (resultSet.next()) {
          //              outputCnt++;
          //            }
          //            if (720 != outputCnt) {
          //              fail(
          //                  "Output count mismatch for fine tuned model."
          //                      + "Expected: "
          //                      + 720
          //                      + ", but got: "
          //                      + outputCnt);
          //            }
          //            return;
          //          }
        }
        TimeUnit.SECONDS.sleep(1); // Wait for 1 second before retrying
      }
      fail(
          String.format(
              "Fine-tuning model did not finish after waiting for %ds.", WAITING_COUNT_SECOND));
    }
  }

  //  @Test
  public void testWeaverMLPFineTune() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE MODEL sundial_weaver_mlp WITH HYPERPARAMETERS (num_train_epochs=2, warmup_steps=80, iter_per_epoch=3000, learning_rate=0.00001, "
              + "finetune_type=weaver_mlp, input_channel=5) FROM MODEL sundial ON DATASET (\'SELECT time, s0, s1, s2, s3, s4 FROM root.AI\')");
      for (int retry = 0; retry < WAITING_COUNT_SECOND; retry++) {
        boolean isActivated = false;
        try (ResultSet resultSet = statement.executeQuery("SHOW MODELS sundial_weaver_mlp")) {
          int modelCnt = 0;
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          checkHeader(resultSetMetaData, "ModelId,ModelType,Category,State");
          while (resultSet.next()) {
            String modelId = resultSet.getString(1);
            String modelType = resultSet.getString(2);
            String category = resultSet.getString(3);
            String state = resultSet.getString(4);

            assertEquals("sundial_weaver_mlp", modelId);
            assertEquals("sundial", modelType);
            assertEquals("fine_tuned", category);
            isActivated = state.equals("active");
            ++modelCnt;
          }
          assertEquals(1, modelCnt);
        }
        if (isActivated) {
          return;
          // TODO: Enable forecast test
          //          final String forecastSQL =
          //              "SELECT * FROM FORECAST("
          //                  + "model_id=>'sundial_weaver_mlp', "
          //                  + "targets=>(SELECT time,s4 FROM root.AI LIMIT 2880) ORDER BY time, "
          //                  + "history_covs=>\'(SELECT time,s0,s1,s2,s3 FROM root.AI LIMIT 2880)
          // ORDER BY time\', "
          //                  + "output_length=>720"
          //                  + ")";
          //          try (ResultSet resultSet = statement.executeQuery(forecastSQL)) {
          //            int outputCnt = 0;
          //            while (resultSet.next()) {
          //              outputCnt++;
          //            }
          //            if (720 != outputCnt) {
          //              fail(
          //                  "Output count mismatch for fine tuned model."
          //                      + "Expected: "
          //                      + 720
          //                      + ", but got: "
          //                      + outputCnt);
          //            }
          //            return;
          //          }
        }
        TimeUnit.SECONDS.sleep(1); // Wait for 1 second before retrying
      }
      fail(
          String.format(
              "Fine-tuning model did not finish after waiting for %ds.", WAITING_COUNT_SECOND));
    }
  }
}
