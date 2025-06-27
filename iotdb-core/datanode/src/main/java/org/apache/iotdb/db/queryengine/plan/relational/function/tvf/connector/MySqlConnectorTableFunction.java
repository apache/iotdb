package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector.converter.ResultSetConverter;
import org.apache.iotdb.rpc.TSStatusCode;
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

import static org.apache.iotdb.db.queryengine.plan.relational.function.tvf.connector.JDBCConnectionPool.translateJDBCTypeToUDFType;
import static org.apache.iotdb.rpc.TSStatusCode.CLOSE_FAILED_IN_EXTERNAL_DB;
import static org.apache.iotdb.rpc.TSStatusCode.EXECUTION_FAILED_IN_EXTERNAL_DB;

public class MySqlConnectorTableFunction implements TableFunction {

  private static class MySqlConnectorTableFunctionHandle implements TableFunctionHandle {
    String sql;
    String url;
    String userName;
    String password;
    int[] types;

    public MySqlConnectorTableFunctionHandle() {}

    public MySqlConnectorTableFunctionHandle(
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
        throw new IoTDBRuntimeException(
            String.format(
                "Error occurred while serializing MySqlConnectorTableFunctionHandle: %s",
                e.getMessage()),
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
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
  private static final String DEFAULT_URL = "jdbc:mysql://localhost:3306";
  private static final String USERNAME = "USERNAME";
  private static final String DEFAULT_USERNAME = "root";
  private static final String PASSWORD = "PASSWORD";
  private static final String DEFAULT_PASSWORD = "root";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder().name(SQL).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(URL)
            .type(Type.STRING)
            .defaultValue(DEFAULT_URL)
            .build(),
        ScalarParameterSpecification.builder()
            .name(USERNAME)
            .type(Type.STRING)
            .defaultValue(DEFAULT_USERNAME)
            .build(),
        ScalarParameterSpecification.builder()
            .name(PASSWORD)
            .type(Type.STRING)
            .defaultValue(DEFAULT_PASSWORD)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {

    String sql = (String) ((ScalarArgument) arguments.get(SQL)).getValue();
    String url = (String) ((ScalarArgument) arguments.get(URL)).getValue();
    String userName = (String) ((ScalarArgument) arguments.get(USERNAME)).getValue();
    String password = (String) ((ScalarArgument) arguments.get(PASSWORD)).getValue();

    DescribedSchema.Builder schemaBuilder = DescribedSchema.builder();
    int[] types;
    try (Connection connection = JDBCConnectionPool.getConnection(url, userName, password);
        PreparedStatement statement = connection.prepareStatement(sql)) {
      ResultSetMetaData metaData = statement.getMetaData();
      types = new int[metaData.getColumnCount()];
      for (int i = 1, size = metaData.getColumnCount(); i <= size; i++) {
        int type = metaData.getColumnType(i);
        schemaBuilder.addField(metaData.getColumnName(i), translateJDBCTypeToUDFType(type));
        types[i - 1] = type;
      }
    } catch (SQLException e) {
      throw new SemanticException(e);
    }
    MySqlConnectorTableFunctionHandle handle =
        new MySqlConnectorTableFunctionHandle(sql, url, userName, password, types);
    return TableFunctionAnalysis.builder()
        .properColumnSchema(schemaBuilder.build())
        .handle(handle)
        .build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MySqlConnectorTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionLeafProcessor getSplitProcessor() {
        return new MysqlProcessor((MySqlConnectorTableFunctionHandle) tableFunctionHandle);
      }
    };
  }

  private static class MysqlProcessor implements TableFunctionLeafProcessor {
    private static final int MAX_TSBLOCK_SIZE_IN_BYTES =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    private static final int MAX_TSBLOCK_LINE_NUMBER =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    private final MySqlConnectorTableFunctionHandle handle;
    private final List<ResultSetConverter> converters;
    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;
    private boolean finished = false;

    MysqlProcessor(MySqlConnectorTableFunctionHandle handle) {
      this.handle = handle;
      this.converters = handle.getConverters();
    }

    @Override
    public void beforeStart() {
      try {
        this.connection =
            JDBCConnectionPool.getConnection(handle.url, handle.userName, handle.password);
        this.statement = connection.createStatement();
        this.resultSet = statement.executeQuery(handle.sql);
      } catch (SQLException e) {
        throw new IoTDBRuntimeException(e, EXECUTION_FAILED_IN_EXTERNAL_DB.getStatusCode(), true);
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
        throw new IoTDBRuntimeException(e, EXECUTION_FAILED_IN_EXTERNAL_DB.getStatusCode(), true);
      }
    }

    @Override
    public boolean isFinish() {
      return finished;
    }

    @Override
    public void beforeDestroy() {
      try {
        if (resultSet != null) {
          resultSet.close();
        }
        if (statement != null) {
          statement.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        throw new IoTDBRuntimeException(e, CLOSE_FAILED_IN_EXTERNAL_DB.getStatusCode(), true);
      }
    }
  }
}
