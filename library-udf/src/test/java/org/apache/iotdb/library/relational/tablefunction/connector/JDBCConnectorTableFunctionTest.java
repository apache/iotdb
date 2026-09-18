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

import org.apache.iotdb.library.relational.tablefunction.connector.exception.CloseFailedInExternalDB;
import org.apache.iotdb.library.relational.tablefunction.connector.exception.ExecutionFailedInExternalDB;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class JDBCConnectorTableFunctionTest {
  @Test
  public void preservesAliasesAndSerializedHandle() {
    AtomicInteger closes = new AtomicInteger();
    ResultSetMetaData metadata =
        proxy(
            ResultSetMetaData.class,
            (p, m, a) -> {
              switch (m.getName()) {
                case "getColumnCount":
                  return 1;
                case "getColumnType":
                  return Types.INTEGER;
                case "getColumnLabel":
                  return "renamed_id";
                case "getColumnName":
                  return "original_id";
                default:
                  throw new AssertionError(m.getName());
              }
            });
    PreparedStatement statement =
        proxy(
            PreparedStatement.class,
            (p, m, a) -> {
              if (m.getName().equals("getMetaData")) return metadata;
              if (m.getName().equals("close")) {
                closes.incrementAndGet();
                return null;
              }
              throw new AssertionError(m.getName());
            });
    StubDriver.connection =
        proxy(
            Connection.class,
            (p, m, a) -> {
              if (m.getName().equals("prepareStatement")) return statement;
              if (m.getName().equals("close")) {
                closes.incrementAndGet();
                return null;
              }
              throw new AssertionError(m.getName());
            });
    BaseJDBCConnectorTableFunction function = function();
    TableFunctionAnalysis analysis =
        function.analyze(arguments("SELECT original_id AS renamed_id"));
    assertEquals(
        "renamed_id", analysis.getProperColumnSchema().get().getFields().get(0).getName().get());
    assertEquals(2, closes.get());
    TableFunctionHandle copy = function.createTableFunctionHandle();
    copy.deserialize(analysis.getTableFunctionHandle().serialize());
    BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle restored =
        (BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle) copy;
    assertEquals("SELECT original_id AS renamed_id", restored.sql);
    assertEquals("jdbc:stub:test", restored.url);
    assertArrayEquals(new int[] {Types.INTEGER}, restored.types);
  }

  @Test
  public void closesConnectionAndStatementWhenExecutionFails() {
    AtomicInteger closes = new AtomicInteger();
    Statement statement =
        proxy(
            Statement.class,
            (p, m, a) -> {
              if (m.getName().equals("executeQuery")) throw new SQLException("query failed");
              if (m.getName().equals("close")) {
                closes.incrementAndGet();
                return null;
              }
              throw new AssertionError(m.getName());
            });
    StubDriver.connection = connection(statement, closes);
    TableFunctionLeafProcessor processor = processor();
    assertThrows(ExecutionFailedInExternalDB.class, processor::beforeStart);
    assertEquals(2, closes.get());
    processor.beforeDestroy();
    assertEquals(2, closes.get());
  }

  @Test
  public void closesRemainingResourcesWhenResultSetCloseFails() {
    AtomicInteger closes = new AtomicInteger();
    ResultSet result =
        proxy(
            ResultSet.class,
            (p, m, a) -> {
              if (m.getName().equals("close")) {
                closes.incrementAndGet();
                throw new SQLException("close failed");
              }
              throw new AssertionError(m.getName());
            });
    Statement statement =
        proxy(
            Statement.class,
            (p, m, a) -> {
              if (m.getName().equals("executeQuery")) return result;
              if (m.getName().equals("close")) {
                closes.incrementAndGet();
                return null;
              }
              throw new AssertionError(m.getName());
            });
    StubDriver.connection = connection(statement, closes);
    TableFunctionLeafProcessor processor = processor();
    processor.beforeStart();
    assertThrows(CloseFailedInExternalDB.class, processor::beforeDestroy);
    assertEquals(3, closes.get());
    processor.beforeDestroy();
    assertEquals(3, closes.get());
  }

  @Test
  public void doesNotLoseRowsBetweenBatches() {
    int rows = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber() * 2 + 1;
    AtomicInteger row = new AtomicInteger();
    ResultSet result =
        proxy(
            ResultSet.class,
            (p, m, a) -> {
              switch (m.getName()) {
                case "next":
                  return row.incrementAndGet() <= rows;
                case "getInt":
                  return row.get();
                case "wasNull":
                  return false;
                case "close":
                  return null;
                default:
                  throw new AssertionError(m.getName());
              }
            });
    Statement statement =
        proxy(
            Statement.class,
            (p, m, a) -> {
              if (m.getName().equals("executeQuery")) return result;
              if (m.getName().equals("close")) return null;
              throw new AssertionError(m.getName());
            });
    StubDriver.connection = connection(statement, new AtomicInteger());
    TableFunctionLeafProcessor processor = processor();
    processor.beforeStart();
    int total = 0;
    int batches = 0;
    while (!processor.isFinish()) {
      assertTrue(++batches < 100);
      TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
      processor.process(Arrays.asList(builder.getValueColumnBuilders()));
      org.apache.tsfile.block.column.Column column = builder.getColumnBuilder(0).build();
      for (int i = 0; i < column.getPositionCount(); i++) assertEquals(++total, column.getInt(i));
    }
    processor.beforeDestroy();
    assertEquals(rows, total);
    assertTrue(batches >= 3);
  }

  @Test
  public void rejectsInvalidArgumentsBeforeConnecting() {
    assertThrows(UDFException.class, () -> function().analyze(arguments(" ")));
  }

  @Test
  public void vendorDriversUseDistinctNamespacesAndRejectOtherUrls() throws Exception {
    for (BaseJDBCConnectorTableFunction function :
        Arrays.asList(
            new PostgreSqlConnectorTableFunction(),
            new OpenGaussConnectorTableFunction(),
            new GaussDBConnectorTableFunction())) {
      Driver driver =
          (Driver)
              Class.forName(function.getDriverClassName()).getDeclaredConstructor().newInstance();
      assertTrue(driver.acceptsURL(function.getDefaultUrl()));
      assertThrows(
          UDFException.class,
          () ->
              JDBCConnectionPool.getConnection(
                  function.getDriverClassName(), "jdbc:unsupported:test", "u", "p"));
    }
  }

  private static BaseJDBCConnectorTableFunction function() {
    return new PostgreSqlConnectorTableFunction() {
      @Override
      String getDriverClassName() {
        return StubDriver.class.getName();
      }
    };
  }

  private static TableFunctionLeafProcessor processor() {
    return function()
        .getProcessorProvider(
            new BaseJDBCConnectorTableFunction.BaseJDBCConnectorTableFunctionHandle(
                "SELECT id", "jdbc:stub:test", "u", "p", new int[] {Types.INTEGER}))
        .getSplitProcessor();
  }

  private static Map<String, Argument> arguments(String sql) {
    Map<String, Argument> values = new HashMap<>();
    values.put("SQL", new ScalarArgument(Type.STRING, sql));
    values.put("URL", new ScalarArgument(Type.STRING, "jdbc:stub:test"));
    values.put("USERNAME", new ScalarArgument(Type.STRING, "u"));
    values.put("PASSWORD", new ScalarArgument(Type.STRING, "p"));
    return values;
  }

  private static Connection connection(Statement statement, AtomicInteger closes) {
    return proxy(
        Connection.class,
        (p, m, a) -> {
          if (m.getName().equals("createStatement")) return statement;
          if (m.getName().equals("close")) {
            closes.incrementAndGet();
            return null;
          }
          throw new AssertionError(m.getName());
        });
  }

  private static <T> T proxy(Class<T> type, InvocationHandler handler) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
  }

  public static class StubDriver implements Driver {
    static Connection connection;

    @Override
    public Connection connect(String url, Properties info) {
      return connection;
    }

    @Override
    public boolean acceptsURL(String url) {
      return url.startsWith("jdbc:stub:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
      return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
      return 1;
    }

    @Override
    public int getMinorVersion() {
      return 0;
    }

    @Override
    public boolean jdbcCompliant() {
      return false;
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getGlobal();
    }
  }
}
