/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.library.relational.tablefunction.connector;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;
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
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFTypeMismatchException;
import org.apache.iotdb.udf.api.type.Type;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCConnectionPool {

  private JDBCConnectionPool() {}

  public static Connection getConnection(
      String driverClassName, String url, String userName, String password) throws SQLException {
    final Driver driver;
    try {
      driver =
          (Driver)
              Class.forName(driverClassName, true, JDBCConnectionPool.class.getClassLoader())
                  .getDeclaredConstructor()
                  .newInstance();
    } catch (ReflectiveOperationException | ClassCastException | LinkageError e) {
      throw new UDFException(
          String.format(
              LibraryUdfMessages
                  .EXCEPTION_FAILED_TO_LOAD_JDBC_DRIVER_ARG_INSTALL_ITS_JAR_IN_THE_UDF_LIBRARY_DIRECTORY_8B1BB953,
              driverClassName),
          e);
    }
    Properties properties = new Properties();
    properties.setProperty("user", userName);
    properties.setProperty("password", password);
    Connection connection = driver.connect(url, properties);
    if (connection == null) {
      throw new UDFException(
          String.format(
              LibraryUdfMessages.EXCEPTION_JDBC_URL_IS_NOT_ACCEPTED_BY_DRIVER_ARG_78687D88,
              driverClassName));
    }
    return connection;
  }

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
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
        return Type.TIMESTAMP;
      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
      case java.sql.Types.BLOB:
        return Type.BLOB;
      case java.sql.Types.BIT:
      case java.sql.Types.BOOLEAN:
        return Type.BOOLEAN;
      default:
        throw new UDFTypeMismatchException(
            String.format(LibraryUdfMessages.EXCEPTION_UNSUPPORTED_JDBC_TYPE_ARG_D8792616, type));
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
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
        return new TimestampConverter();
      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
        return new BinaryConverter();
      case java.sql.Types.BLOB:
        return new BlobConverter();
      case java.sql.Types.BIT:
      case java.sql.Types.BOOLEAN:
        return new BooleanConverter();
      default:
        throw new UDFTypeMismatchException(
            String.format(LibraryUdfMessages.EXCEPTION_UNSUPPORTED_JDBC_TYPE_ARG_D8792616, type));
    }
  }
}
