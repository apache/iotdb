package org.apache.iotdb.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.sql.*;
import java.util.*;

public class IoTDBSchema extends AbstractSchema {
  final Connection connection;
  private Map<String, Table> tableMap;
  private final SchemaPlus parentSchema;
  final String name;

  /**
   * Creates a IoTDB schema.
   *
   * @param host IoTDB host, e.g. "localhost"
   * @param port IoTDB port, e.g. 6667
   * @param username IoTDB username
   * @param password IoTDB password
   */
  public IoTDBSchema(String host, int port, String username, String password,
           SchemaPlus parentSchema, String name) {
    super();
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      this.connection = DriverManager
              .getConnection(Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    this.parentSchema = parentSchema;
    this.name = name;
  }

  RelProtoDataType getRelDataType(String storageGroup) throws SQLException {
    final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    // add time, device columns in relational table
    fieldInfo.add(IoTDBConstant.TimeColumn, typeFactory.createSqlType(IoTDBFieldType.INT64.getSqlType()));
    fieldInfo.add(IoTDBConstant.DeviceColumn, typeFactory.createSqlType(IoTDBFieldType.TEXT.getSqlType()));

    // get one device in this storage group
    Statement statement = connection.createStatement();
    boolean hasDevices = statement.execute("show devices " + storageGroup);
    if(hasDevices){
      ResultSet devices = statement.getResultSet();
      devices.next();
      boolean hasTS = statement.execute("show timeseries " + devices.getString(2));
      if(hasTS){
        ResultSet timeseries = statement.getResultSet();
        while (timeseries.next()) {
          String sensorName = timeseries.getString(2);
          IoTDBFieldType sensorType = IoTDBFieldType.of(timeseries.getString(4));
          int index = sensorName.lastIndexOf('.');
          fieldInfo.add(sensorName.substring(index + 1), typeFactory.createSqlType(sensorType.getSqlType()));
        }
      }
    }

    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  @Override
  protected Map<String, Table> getTableMap() {
    try {
      if(tableMap == null){
        tableMap = createTableMap();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return tableMap;
  }

  public Map<String, Table> createTableMap() throws SQLException {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    List<String> storageGroups = new ArrayList<>();
    Statement statement = connection.createStatement();
    boolean hasResultSet = statement.execute("show storage group");
    if(hasResultSet) {
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        storageGroups.add(resultSet.getString(2).toLowerCase());
      }
    }
    for (String storageGroup : storageGroups) {
      builder.put(storageGroup, new IoTDBTable(this, storageGroup));
    }

    return builder.build();
  }

}

// End IoTDBSchema.java