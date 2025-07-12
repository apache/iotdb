package org.apache.iotdb.ainode.utils;

import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AINodeTestUtils {

  public static final String EXAMPLE_MODEL_PATH =
      System.getProperty("user.dir")
          + File.separator
          + "src"
          + File.separator
          + "test"
          + File.separator
          + "resources"
          + File.separator
          + "ainode-example";

  public static void checkHeader(ResultSetMetaData resultSetMetaData, String title)
      throws SQLException {
    String[] headers = title.split(",");
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(headers[i - 1], resultSetMetaData.getColumnName(i));
    }
  }

  public static void errorTest(Statement statement, String sql, String errorMessage) {
    try (ResultSet ignored = statement.executeQuery(sql)) {
      fail("There should be an exception");
    } catch (SQLException e) {
      assertEquals(errorMessage, e.getMessage());
    }
  }

  public static class FakeModelInfo {

    private final String modelId;
    private final String modelType;
    private final String category;
    private final String state;

    public FakeModelInfo(String modelId, String modelType, String category, String state) {
      this.modelId = modelId;
      this.modelType = modelType;
      this.category = category;
      this.state = state;
    }

    public String getModelId() {
      return modelId;
    }

    public String getModelType() {
      return modelType;
    }

    public String getCategory() {
      return category;
    }

    public String getState() {
      return state;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FakeModelInfo modelInfo = (FakeModelInfo) o;
      return Objects.equals(modelId, modelInfo.modelId)
          && Objects.equals(modelType, modelInfo.modelType)
          && Objects.equals(category, modelInfo.category)
          && Objects.equals(state, modelInfo.state);
    }

    @Override
    public int hashCode() {
      return Objects.hash(modelId, modelType, category, state);
    }
  }
}
