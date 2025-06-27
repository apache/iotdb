package org.apache.iotdb.library.relational.tablefunction.connector;

import org.apache.iotdb.library.relational.tablefunction.connector.converter.BinaryConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.BlobConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.BooleanConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.DateConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.DoubleConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.FloatConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.Int32Converter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.Int64Converter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.ResultSetConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.StringConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.TimeConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.converter.TimestampConverter;
import org.apache.iotdb.udf.api.exception.UDFTypeMismatchException;
import org.apache.iotdb.udf.api.type.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnectionPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCConnectionPool.class);

  private JDBCConnectionPool() {}

  static {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize mysql JDBC driver", e);
    }
  }

  public static Connection getConnection(String url, String userName, String password)
      throws SQLException {
    return DriverManager.getConnection(url, userName, password);
  }

  private static class ConnectionWrapper {}

  public static Type translateJDBCTypeToUDFType(int type) {
    switch (type) {
      case java.sql.Types.TINYINT:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.INTEGER:
        return Type.INT32;
      case java.sql.Types.BIGINT:
        return Type.INT64;
      case java.sql.Types.FLOAT:
        return Type.FLOAT;
      case java.sql.Types.DOUBLE:
      case java.sql.Types.REAL:
      case java.sql.Types.NUMERIC:
      case java.sql.Types.DECIMAL:
        return Type.DOUBLE;
      case java.sql.Types.CHAR:
      case java.sql.Types.VARCHAR:
      case java.sql.Types.LONGVARCHAR:
      case java.sql.Types.NCHAR:
      case java.sql.Types.NVARCHAR:
      case java.sql.Types.LONGNVARCHAR:
        return Type.STRING;
      case java.sql.Types.DATE:
        return Type.DATE;
      case java.sql.Types.TIME:
      case java.sql.Types.TIMESTAMP:
        return Type.TIMESTAMP;
      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
      case java.sql.Types.BLOB:
        return Type.BLOB;
      case java.sql.Types.BOOLEAN:
        return Type.BOOLEAN;
      default:
        throw new UDFTypeMismatchException("Unsupported JDBC type: " + type);
    }
  }

  public static ResultSetConverter getResultSetConverter(int type) {
    switch (type) {
      case java.sql.Types.TINYINT:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.INTEGER:
        return new Int32Converter();
      case java.sql.Types.BIGINT:
        return new Int64Converter();
      case java.sql.Types.FLOAT:
        return new FloatConverter();
      case java.sql.Types.DOUBLE:
      case java.sql.Types.REAL:
      case java.sql.Types.NUMERIC:
      case java.sql.Types.DECIMAL:
        return new DoubleConverter();
      case java.sql.Types.CHAR:
      case java.sql.Types.VARCHAR:
      case java.sql.Types.LONGVARCHAR:
      case java.sql.Types.NCHAR:
      case java.sql.Types.NVARCHAR:
      case java.sql.Types.LONGNVARCHAR:
        return new StringConverter();
      case java.sql.Types.DATE:
        return new DateConverter();
      case java.sql.Types.TIME:
        return new TimeConverter();
      case java.sql.Types.TIMESTAMP:
        return new TimestampConverter();
      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
        return new BinaryConverter();
      case java.sql.Types.BLOB:
        return new BlobConverter();
      case java.sql.Types.BOOLEAN:
        return new BooleanConverter();
      default:
        throw new UDFTypeMismatchException("Unsupported JDBC type: " + type);
    }
  }
}
