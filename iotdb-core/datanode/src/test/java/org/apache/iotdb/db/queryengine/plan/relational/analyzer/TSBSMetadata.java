/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.OperatorNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTSBSDataPartition.T1_DEVICE_1;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTSBSDataPartition.T1_DEVICE_2;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTSBSDataPartition.T1_DEVICE_3;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTSBSDataPartition.T2_DEVICE_1;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTSBSDataPartition.T2_DEVICE_2;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTSBSDataPartition.T2_DEVICE_3;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.getFunctionType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isOneNumericType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isTwoNumericType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isTwoTypeComparable;
import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class TSBSMetadata implements Metadata {

  private final TypeManager typeManager = new InternalTypeManager();

  public static final String DB1 = "tsbs";
  public static final String TABLE_DIAGNOSTICS = "diagnostics";
  public static final String TABLE_READINGS = "readings";

  public static final String TIME = "time";
  private static final ColumnMetadata TIME_CM = new ColumnMetadata(TIME, TIMESTAMP);

  // ID columns
  public static final String NAME = "name";
  private static final ColumnMetadata NAME_CM = new ColumnMetadata(NAME, TEXT);
  public static final String FLEET = "fleet";
  private static final ColumnMetadata FLEET_CM = new ColumnMetadata(FLEET, TEXT);
  public static final String DRIVER = "driver";
  private static final ColumnMetadata DRIVER_CM = new ColumnMetadata(DRIVER, TEXT);
  public static final String MODEL = "model";
  private static final ColumnMetadata MODEL_CM = new ColumnMetadata(MODEL, TEXT);

  // ATTRIBUTE columns
  public static final String DEVICE_VERSION = "device_version";
  private static final ColumnMetadata DEVICE_VERSION_CM = new ColumnMetadata(DEVICE_VERSION, TEXT);
  public static final String LOAD_CAPACITY = "load_capacity";
  private static final ColumnMetadata LOAD_CAPACITY_CM = new ColumnMetadata(LOAD_CAPACITY, DOUBLE);
  public static final String FUEL_CAPACITY = "fuel_capacity";
  private static final ColumnMetadata FUEL_CAPACITY_CM = new ColumnMetadata(FUEL_CAPACITY, DOUBLE);
  public static final String NOMINAL_FUEL_CONSUMPTION = "nominal_fuel_consumption";
  private static final ColumnMetadata NOMINAL_FUEL_CONSUMPTION_CM =
      new ColumnMetadata(NOMINAL_FUEL_CONSUMPTION, DOUBLE);

  // TABLE_DIAGNOSTICS measurement columns
  public static final String FUEL_STATE = "fuel_state";
  private static final ColumnMetadata FUEL_STATE_CM = new ColumnMetadata(FUEL_STATE, DOUBLE);
  public static final String CURRENT_LOAD = "current_load";
  private static final ColumnMetadata CURRENT_LOAD_CM = new ColumnMetadata(CURRENT_LOAD, INT32);
  public static final String STATUS = "status";
  private static final ColumnMetadata STATUS_CM = new ColumnMetadata(STATUS, INT32);

  // TABLE_READINGS measurement columns
  public static final String LATITUDE = "latitude";
  private static final ColumnMetadata LATITUDE_CM = new ColumnMetadata(LATITUDE, DOUBLE);
  public static final String LONGITUDE = "longitude";
  private static final ColumnMetadata LONGITUDE_CM = new ColumnMetadata(LONGITUDE, DOUBLE);
  public static final String ELEVATION = "elevation";
  private static final ColumnMetadata ELEVATION_CM = new ColumnMetadata(ELEVATION, INT32);
  public static final String VELOCITY = "velocity";
  private static final ColumnMetadata VELOCITY_CM = new ColumnMetadata(VELOCITY, INT32);
  public static final String HEADING = "heading";
  private static final ColumnMetadata HEADING_CM = new ColumnMetadata(HEADING, INT32);
  public static final String GRADE = "grade";
  private static final ColumnMetadata GRADE_CM = new ColumnMetadata(GRADE, INT32);
  public static final String FUEL_CONSUMPTION = "fuel_consumption";
  private static final ColumnMetadata FUEL_CONSUMPTION_CM =
      new ColumnMetadata(FUEL_CONSUMPTION, INT32);

  @Override
  public boolean tableExists(QualifiedObjectName name) {
    return name.getDatabaseName().equalsIgnoreCase(DB1)
        && (name.getObjectName().equalsIgnoreCase(TABLE_DIAGNOSTICS)
            || name.getObjectName().equalsIgnoreCase(TABLE_READINGS));
  }

  @Override
  public Optional<TableSchema> getTableSchema(SessionInfo session, QualifiedObjectName name) {
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(
        ColumnSchema.builder(TIME_CM).setColumnCategory(TsTableColumnCategory.TIME).build());
    columnSchemas.add(
        ColumnSchema.builder(NAME_CM).setColumnCategory(TsTableColumnCategory.ID).build());
    columnSchemas.add(
        ColumnSchema.builder(FLEET_CM).setColumnCategory(TsTableColumnCategory.ID).build());
    columnSchemas.add(
        ColumnSchema.builder(DRIVER_CM).setColumnCategory(TsTableColumnCategory.ID).build());
    columnSchemas.add(
        ColumnSchema.builder(MODEL_CM).setColumnCategory(TsTableColumnCategory.ID).build());
    columnSchemas.add(
        ColumnSchema.builder(DEVICE_VERSION_CM)
            .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
            .build());
    columnSchemas.add(
        ColumnSchema.builder(LOAD_CAPACITY_CM)
            .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
            .build());
    columnSchemas.add(
        ColumnSchema.builder(FUEL_CAPACITY_CM)
            .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
            .build());
    columnSchemas.add(
        ColumnSchema.builder(NOMINAL_FUEL_CONSUMPTION_CM)
            .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
            .build());

    if (name.getObjectName().equalsIgnoreCase(TABLE_DIAGNOSTICS)) {
      columnSchemas.add(
          ColumnSchema.builder(FUEL_STATE_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(CURRENT_LOAD_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(STATUS_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      return Optional.of(new TableSchema(TABLE_DIAGNOSTICS, columnSchemas));
    } else if (name.getObjectName().equalsIgnoreCase(TABLE_READINGS)) {
      columnSchemas.add(
          ColumnSchema.builder(LATITUDE_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(LONGITUDE_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(ELEVATION_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(VELOCITY_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(HEADING_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(GRADE_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      columnSchemas.add(
          ColumnSchema.builder(FUEL_CONSUMPTION_CM)
              .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
              .build());
      return Optional.of(new TableSchema(TABLE_READINGS, columnSchemas));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Type getOperatorReturnType(OperatorType operatorType, List<? extends Type> argumentTypes)
      throws OperatorNotFoundException {

    switch (operatorType) {
      case ADD:
      case SUBTRACT:
      case MULTIPLY:
      case DIVIDE:
      case MODULUS:
        if (!isTwoNumericType(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        return DOUBLE;
      case NEGATION:
        if (!isOneNumericType(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have one numeric operands."));
        }
        return DOUBLE;
      case EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        if (!isTwoTypeComparable(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two comparable operands."));
        }
        return BOOLEAN;
      default:
        throw new OperatorNotFoundException(
            operatorType, argumentTypes, new UnsupportedOperationException());
    }
  }

  @Override
  public Type getFunctionReturnType(String functionName, List<? extends Type> argumentTypes) {
    return getFunctionType(functionName, argumentTypes);
  }

  @Override
  public boolean isAggregationFunction(
      SessionInfo session, String functionName, AccessControl accessControl) {
    return BuiltinAggregationFunction.getNativeFunctionNames()
        .contains(functionName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Type getType(TypeSignature signature) throws TypeNotFoundException {
    return typeManager.getType(signature);
  }

  @Override
  public boolean canCoerce(Type from, Type to) {
    return true;
  }

  @Override
  public IPartitionFetcher getPartitionFetcher() {
    return getFakePartitionFetcher();
  }

  @Override
  public List<DeviceEntry> indexScan(
      QualifiedObjectName tableName,
      List<Expression> expressionList,
      List<String> attributeColumns,
      MPPQueryContext context) {
    if (expressionList.size() == 2
        && expressionList.get(0).toString().equals("(\"fleet\" = 'South')")
        && expressionList.get(1).toString().equals("(NOT (\"name\" IS NULL))")
        && attributeColumns.isEmpty()) {
      // r01, r02
      return ImmutableList.of(
          new DeviceEntry(new StringArrayDeviceID(T1_DEVICE_1.split("\\.")), ImmutableList.of()),
          new DeviceEntry(new StringArrayDeviceID(T1_DEVICE_2.split("\\.")), ImmutableList.of()));
    } else if (expressionList.size() == 1
        && expressionList.get(0).toString().equals("(\"fleet\" = 'South')")
        && attributeColumns.size() == 1
        && attributeColumns.get(0).equals("load_capacity")) {
      // r03
      return ImmutableList.of(
          new DeviceEntry(
              new StringArrayDeviceID(T1_DEVICE_1.split("\\.")), ImmutableList.of("2000")),
          new DeviceEntry(
              new StringArrayDeviceID(T1_DEVICE_2.split("\\.")), ImmutableList.of("1000")));
    } else {
      // others (The return result maybe not correct in actual, but it is convenient for test of
      // DistributionPlan)
      return Arrays.asList(
          new DeviceEntry(
              new StringArrayDeviceID(T1_DEVICE_1.split("\\.")), ImmutableList.of("", "")),
          new DeviceEntry(
              new StringArrayDeviceID(T1_DEVICE_2.split("\\.")), ImmutableList.of("", "")),
          new DeviceEntry(
              new StringArrayDeviceID(T1_DEVICE_3.split("\\.")), ImmutableList.of("", "")),
          new DeviceEntry(
              new StringArrayDeviceID(T2_DEVICE_1.split("\\.")), ImmutableList.of("", "")),
          new DeviceEntry(
              new StringArrayDeviceID(T2_DEVICE_2.split("\\.")), ImmutableList.of("", "")),
          new DeviceEntry(
              new StringArrayDeviceID(T2_DEVICE_3.split("\\.")), ImmutableList.of("", "")));
    }
  }

  @Override
  public Optional<TableSchema> validateTableHeaderSchema(
      String database, TableSchema tableSchema, MPPQueryContext context, boolean allowCreateTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateDeviceSchema(
      ITableDeviceSchemaValidation schemaValidation, MPPQueryContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(
      String database, List<IDeviceID> deviceIDList, String userName) {
    return null;
  }

  @Override
  public SchemaPartition getSchemaPartition(String database, List<IDeviceID> deviceIDList) {
    return null;
  }

  @Override
  public SchemaPartition getSchemaPartition(String database) {
    return null;
  }

  @Override
  public DataPartition getDataPartition(
      String database, List<DataPartitionQueryParam> sgNameToQueryParamsMap) {
    return DATA_PARTITION;
  }

  @Override
  public DataPartition getDataPartitionWithUnclosedTimeRange(
      String database, List<DataPartitionQueryParam> sgNameToQueryParamsMap) {
    return DATA_PARTITION;
  }

  @Override
  public boolean canUseStatistics(String name) {
    return BuiltinAggregationFunction.canUseStatistics(name);
  }

  private static final DataPartition DATA_PARTITION =
      MockTSBSDataPartition.constructDataPartition();

  private static final SchemaPartition SCHEMA_PARTITION =
      MockTSBSDataPartition.constructSchemaPartition();

  private static IPartitionFetcher getFakePartitionFetcher() {

    return new IPartitionFetcher() {

      @Override
      public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
        return SCHEMA_PARTITION;
      }

      @Override
      public SchemaPartition getOrCreateSchemaPartition(
          PathPatternTree patternTree, String userName) {
        return SCHEMA_PARTITION;
      }

      @Override
      public DataPartition getDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return DATA_PARTITION;
      }

      @Override
      public DataPartition getDataPartitionWithUnclosedTimeRange(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return DATA_PARTITION;
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return DATA_PARTITION;
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          List<DataPartitionQueryParam> dataPartitionQueryParams, String userName) {
        return DATA_PARTITION;
      }

      @Override
      public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
          PathPatternTree patternTree, PathPatternTree scope, Integer level) {
        return null;
      }

      @Override
      public boolean updateRegionCache(TRegionRouteReq req) {
        return false;
      }

      @Override
      public void invalidAllCache() {}

      @Override
      public SchemaPartition getOrCreateSchemaPartition(
          String database, List<IDeviceID> deviceIDList, String userName) {
        return SCHEMA_PARTITION;
      }

      @Override
      public SchemaPartition getSchemaPartition(String database, List<IDeviceID> deviceIDList) {
        return SCHEMA_PARTITION;
      }

      @Override
      public SchemaPartition getSchemaPartition(String database) {
        return SCHEMA_PARTITION;
      }
    };
  }
}
