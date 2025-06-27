package org.apache.iotdb.library.relational.tablefunction.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlConnectorTableFunction extends BaseJDBCConnectorTableFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorTableFunction.class);

  static {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize mysql JDBC driver", e);
    }
  }

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
    return DEFAULT_USERNAME;
  }

  @Override
  String getDefaultPassword() {
    return DEFAULT_PASSWORD;
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
