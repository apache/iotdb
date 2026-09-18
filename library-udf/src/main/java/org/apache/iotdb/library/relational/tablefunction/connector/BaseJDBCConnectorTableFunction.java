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
import org.apache.iotdb.library.relational.tablefunction.connector.converter.ResultSetConverter;
import org.apache.iotdb.library.relational.tablefunction.connector.exception.CloseFailedInExternalDB;
import org.apache.iotdb.library.relational.tablefunction.connector.exception.ExecutionFailedInExternalDB;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.library.relational.tablefunction.connector.JDBCConnectionPool.translateJDBCTypeToUDFType;

abstract class BaseJDBCConnectorTableFunction implements TableFunction {

  static class BaseJDBCConnectorTableFunctionHandle implements TableFunctionHandle {
    String sql;
    String url;
    String userName;
    String password;
    int[] types;

    public BaseJDBCConnectorTableFunctionHandle() {}

    public BaseJDBCConnectorTableFunctionHandle(
        String sql, String url, String userName, String password, int[] types) {
      this.sql = sql;
      this.url = url;
      this.userName = userName;
      this.password = password;
      this.types = types;
    }

    List<ResultSetConverter> getConverters() {
      List<ResultSetConverter> converters = new ArrayList<>(types.length);
      for (int type : types) {
        converters.add(JDBCConnectionPool.getResultSetConverter(type));
      }
      return converters;
    }

    @Override
    public byte[] serialize() {
      try (PublicBAOS publicBAOS = new PublicBAOS();
          DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
        ReadWriteIOUtils.write(sql, outputStream);
        ReadWriteIOUtils.write(url, outputStream);
        ReadWriteIOUtils.write(userName, outputStream);
        ReadWriteIOUtils.write(password, outputStream);
        ReadWriteIOUtils.write(types.length, outputStream);
        for (int type : types) {
          ReadWriteIOUtils.write(type, outputStream);
        }
        outputStream.flush();
        return publicBAOS.toByteArray();
      } catch (IOException e) {
        throw new UDFException(
            String.format(
                LibraryUdfMessages
                    .EXCEPTION_FAILED_TO_SERIALIZE_JDBC_TABLE_FUNCTION_HANDLE_ARG_1023C78A,
                e.getMessage()));
      }
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      this.sql = ReadWriteIOUtils.readString(buffer);
      this.url = ReadWriteIOUtils.readString(buffer);
      this.userName = ReadWriteIOUtils.readString(buffer);
      this.password = ReadWriteIOUtils.readString(buffer);
      this.types = new int[ReadWriteIOUtils.readInt(buffer)];
      for (int i = 0; i < types.length; i++) {
        types[i] = ReadWriteIOUtils.readInt(buffer);
      }
    }
  }

  private static final String SQL = "SQL";
  private static final String URL = "URL";
  private static final String USERNAME = "USERNAME";
  private static final String PASSWORD = "PASSWORD";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder().name(SQL).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(URL)
            .type(Type.STRING)
            .defaultValue(getDefaultUrl())
            .build(),
        ScalarParameterSpecification.builder()
            .name(USERNAME)
            .type(Type.STRING)
            .defaultValue(getDefaultUser())
            .build(),
        ScalarParameterSpecification.builder()
            .name(PASSWORD)
            .type(Type.STRING)
            .defaultValue(getDefaultPassword())
            .build());
  }

  abstract String getDefaultUrl();

  abstract String getDefaultUser();

  abstract String getDefaultPassword();

  abstract String getDriverClassName();

  abstract String getDBName();

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {

    String sql = (String) ((ScalarArgument) arguments.get(SQL)).getValue();
    String url = (String) ((ScalarArgument) arguments.get(URL)).getValue();
    String userName = (String) ((ScalarArgument) arguments.get(USERNAME)).getValue();
    String password = (String) ((ScalarArgument) arguments.get(PASSWORD)).getValue();

    if (sql == null
        || sql.trim().isEmpty()
        || url == null
        || url.trim().isEmpty()
        || userName == null
        || password == null) {
      throw new UDFException(
          LibraryUdfMessages
              .EXCEPTION_SQL_AND_URL_MUST_BE_NON_EMPTY_STRINGS_USERNAME_AND_PASSWORD_MUST_NOT_BE_NULL_A253F77B);
    }
    DescribedSchema.Builder schemaBuilder = DescribedSchema.builder();
    int[] types = buildResultHeaders(schemaBuilder, sql, url, userName, password);
    BaseJDBCConnectorTableFunctionHandle handle =
        new BaseJDBCConnectorTableFunctionHandle(sql, url, userName, password, types);
    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .handle(handle)
        .build();
  }

  int[] buildResultHeaders(
      DescribedSchema.Builder schemaBuilder,
      String sql,
      String url,
      String userName,
      String password) {
    int[] types;
    try (Connection connection =
            JDBCConnectionPool.getConnection(getDriverClassName(), url, userName, password);
        PreparedStatement statement = connection.prepareStatement(sql)) {
      ResultSetMetaData metaData = statement.getMetaData();
      if (metaData == null || metaData.getColumnCount() == 0) {
        throw new UDFException(
            LibraryUdfMessages
                .EXCEPTION_SQL_MUST_BE_A_QUERY_THAT_RETURNS_AT_LEAST_ONE_COLUMN_1AF6E91B);
      }
      types = new int[metaData.getColumnCount()];
      for (int i = 1, size = metaData.getColumnCount(); i <= size; i++) {
        int type = metaData.getColumnType(i);
        schemaBuilder.addField(metaData.getColumnLabel(i), translateJDBCTypeToUDFType(type));
        types[i - 1] = type;
      }
      return types;
    } catch (SQLException e) {
      throw new UDFException(
          String.format(
              LibraryUdfMessages.EXCEPTION_FAILED_TO_READ_JDBC_RESULT_METADATA_ARG_16D4E50C,
              e.getMessage()),
          e);
    }
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new BaseJDBCConnectorTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionLeafProcessor getSplitProcessor() {
        return getProcessor((BaseJDBCConnectorTableFunctionHandle) tableFunctionHandle);
      }
    };
  }

  JDBCProcessor getProcessor(BaseJDBCConnectorTableFunctionHandle tableFunctionHandle) {
    return new JDBCProcessor(tableFunctionHandle, getDriverClassName(), getDBName());
  }

  static class JDBCProcessor implements TableFunctionLeafProcessor {
    private static final int MAX_TSBLOCK_SIZE_IN_BYTES =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    private static final int MAX_TSBLOCK_LINE_NUMBER =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    private final BaseJDBCConnectorTableFunctionHandle handle;
    private final List<ResultSetConverter> converters;
    private final String driverClassName;
    private final String dbName;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private boolean finished = false;

    JDBCProcessor(
        BaseJDBCConnectorTableFunctionHandle handle, String driverClassName, String dbName) {
      this.driverClassName = driverClassName;
      this.dbName = dbName;
      this.handle = handle;
      this.converters = handle.getConverters();
    }

    @Override
    public void beforeStart() {
      try {
        this.connection =
            JDBCConnectionPool.getConnection(
                driverClassName, handle.url, handle.userName, handle.password);
        this.statement = connection.createStatement();
        this.resultSet = statement.executeQuery(handle.sql);
      } catch (SQLException | RuntimeException e) {
        try {
          beforeDestroy();
        } catch (RuntimeException closeError) {
          e.addSuppressed(closeError);
        }
        throw new ExecutionFailedInExternalDB(dbName, e);
      }
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      finished = true;
      try {
        int count = 0;
        while (resultSet.next()) {
          long retainedSize = 0;
          for (int i = 0, size = columnBuilders.size(); i < size; i++) {
            converters.get(i).append(resultSet, i + 1, columnBuilders.get(i));
            retainedSize += columnBuilders.get(i).getRetainedSizeInBytes();
          }
          count++;
          if (retainedSize >= MAX_TSBLOCK_SIZE_IN_BYTES || count >= MAX_TSBLOCK_LINE_NUMBER) {
            finished = false;
            break;
          }
        }
      } catch (SQLException e) {
        throw new ExecutionFailedInExternalDB(dbName, e);
      }
    }

    @Override
    public boolean isFinish() {
      return finished;
    }

    @Override
    public void beforeDestroy() {
      finished = true;
      SQLException failure = null;
      // Attempt all closes even if the result set or statement fails to close.
      for (AutoCloseable resource : new AutoCloseable[] {resultSet, statement, connection}) {
        if (resource != null) {
          try {
            resource.close();
          } catch (Exception e) {
            if (failure == null) {
              failure = new SQLException(e);
            } else {
              failure.addSuppressed(e);
            }
          }
        }
      }
      resultSet = null;
      statement = null;
      connection = null;
      if (failure != null) {
        throw new CloseFailedInExternalDB(dbName, failure);
      }
    }
  }
}
