package org.apache.iotdb.library.relational.tablefunction.connector;

public class MySqlConnectorTableFunction extends BaseJDBCConnectorTableFunction {

  private static final String DEFAULT_URL =
      "jdbc:mysql://localhost:3306?allowPublicKeyRetrieval=true";
  private static final String DEFAULT_USERNAME = "root";
  private static final String DEFAULT_PASSWORD = "root";
  private static final String MYSQL = "MySQL";

  @Override
  String getDefaultUrl() {
    return DEFAULT_URL;
  }

  @Override
  String getDefaultUser() {
    return DEFAULT_PASSWORD;
  }

  @Override
  String getDefaultPassword() {
    return DEFAULT_USERNAME;
  }

  @Override
  BaseJDBCConnectorTableFunction.JDBCProcessor getProcessor(
      BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle tableFunctionHandle) {
    return new MysqlProcessor(tableFunctionHandle);
  }

  private static class MysqlProcessor extends JDBCProcessor {

    MysqlProcessor(BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle handle) {
      super(handle);
    }

    @Override
    String getDBName() {
      return MYSQL;
    }
  }
}
