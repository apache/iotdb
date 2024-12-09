/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.SchemaNodeManagementPartition;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.InformationSchemaTable;
import org.apache.iotdb.commons.schema.table.TsTable;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.InformationSchemaTable.INFORMATION_SCHEMA;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_1;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_1_ATTRIBUTES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_2;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_2_ATTRIBUTES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_3;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_3_ATTRIBUTES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_4;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_4_ATTRIBUTES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_5;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_5_ATTRIBUTES;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_6;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition.DEVICE_6_ATTRIBUTES;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.getFunctionType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isOneNumericType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isTwoNumericType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isTwoTypeComparable;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

public class TestMatadata implements Metadata {

  private final TypeManager typeManager = new InternalTypeManager();

  public static final String DB1 = "testdb";
  public static final String TABLE1 = "table1";
  public static final String TIME = "time";
  private static final String TAG1 = "tag1";
  private static final String TAG2 = "tag2";
  private static final String TAG3 = "tag3";
  private static final String ATTR1 = "attr1";
  private static final String ATTR2 = "attr2";
  private static final String S1 = "s1";
  private static final String S2 = "s2";
  private static final String S3 = "s3";
  private static final ColumnMetadata TIME_CM = new ColumnMetadata(TIME, TIMESTAMP);
  private static final ColumnMetadata TAG1_CM = new ColumnMetadata(TAG1, StringType.STRING);
  private static final ColumnMetadata TAG2_CM = new ColumnMetadata(TAG2, StringType.STRING);
  private static final ColumnMetadata TAG3_CM = new ColumnMetadata(TAG3, StringType.STRING);
  private static final ColumnMetadata ATTR1_CM = new ColumnMetadata(ATTR1, StringType.STRING);
  private static final ColumnMetadata ATTR2_CM = new ColumnMetadata(ATTR2, StringType.STRING);
  private static final ColumnMetadata S1_CM = new ColumnMetadata(S1, INT64);
  private static final ColumnMetadata S2_CM = new ColumnMetadata(S2, INT64);
  private static final ColumnMetadata S3_CM = new ColumnMetadata(S3, DOUBLE);

  public static final String DB2 = "db2";
  public static final String TABLE2 = "table2";

  @Override
  public boolean tableExists(QualifiedObjectName name) {
    return name.getDatabaseName().equalsIgnoreCase(DB1)
        && name.getObjectName().equalsIgnoreCase(TABLE1);
  }

  @Override
  public Optional<TableSchema> getTableSchema(SessionInfo session, QualifiedObjectName name) {
    if (name.getDatabaseName().equals(INFORMATION_SCHEMA)) {
      TsTable table = InformationSchemaTable.getTableFromStringValue(name.getObjectName());
      if (table == null) {
        return Optional.empty();
      }
      List<ColumnSchema> columnSchemaList =
          table.getColumnList().stream()
              .map(
                  o ->
                      new ColumnSchema(
                          o.getColumnName(),
                          TypeFactory.getType(o.getDataType()),
                          false,
                          o.getColumnCategory()))
              .collect(Collectors.toList());
      return Optional.of(new TableSchema(table.getTableName(), columnSchemaList));
    }

    List<ColumnSchema> columnSchemas =
        Arrays.asList(
            ColumnSchema.builder(TIME_CM).setColumnCategory(TsTableColumnCategory.TIME).build(),
            ColumnSchema.builder(TAG1_CM).setColumnCategory(TsTableColumnCategory.ID).build(),
            ColumnSchema.builder(TAG2_CM).setColumnCategory(TsTableColumnCategory.ID).build(),
            ColumnSchema.builder(TAG3_CM).setColumnCategory(TsTableColumnCategory.ID).build(),
            ColumnSchema.builder(ATTR1_CM)
                .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
                .build(),
            ColumnSchema.builder(ATTR2_CM)
                .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
                .build(),
            ColumnSchema.builder(S1_CM)
                .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
                .build(),
            ColumnSchema.builder(S2_CM)
                .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
                .build(),
            ColumnSchema.builder(S3_CM)
                .setColumnCategory(TsTableColumnCategory.MEASUREMENT)
                .build());

    return Optional.of(new TableSchema(TABLE1, columnSchemas));
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

    if (expressionList.size() == 2) {
      if (compareEqualsMatch(expressionList.get(0), "tag1", "beijing")
              && compareEqualsMatch(expressionList.get(1), "tag2", "A1")
          || compareEqualsMatch(expressionList.get(1), "tag1", "beijing")
              && compareEqualsMatch(expressionList.get(0), "tag2", "A1")) {
        return Collections.singletonList(
            new DeviceEntry(new StringArrayDeviceID(DEVICE_1.split("\\.")), DEVICE_1_ATTRIBUTES));
      }
      if (compareEqualsMatch(expressionList.get(0), "tag1", "shanghai")
              && compareEqualsMatch(expressionList.get(1), "tag2", "B3")
          || compareEqualsMatch(expressionList.get(1), "tag1", "shanghai")
              && compareEqualsMatch(expressionList.get(0), "tag2", "B3")) {
        return Collections.singletonList(
            new DeviceEntry(new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_1_ATTRIBUTES));
      }

    } else if (expressionList.size() == 1) {
      if (compareEqualsMatch(expressionList.get(0), "tag1", "shanghai")) {
        return Arrays.asList(
            new DeviceEntry(new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_4_ATTRIBUTES),
            new DeviceEntry(new StringArrayDeviceID(DEVICE_3.split("\\.")), DEVICE_3_ATTRIBUTES));
      }
      if (compareEqualsMatch(expressionList.get(0), "tag1", "shenzhen")) {
        return Arrays.asList(
            new DeviceEntry(new StringArrayDeviceID(DEVICE_6.split("\\.")), DEVICE_6_ATTRIBUTES),
            new DeviceEntry(new StringArrayDeviceID(DEVICE_5.split("\\.")), DEVICE_5_ATTRIBUTES));
      }
      if (compareNotEqualsMatch(expressionList.get(0), "tag1", "shenzhen")) {
        return Arrays.asList(
            new DeviceEntry(new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_4_ATTRIBUTES),
            new DeviceEntry(new StringArrayDeviceID(DEVICE_1.split("\\.")), DEVICE_1_ATTRIBUTES),
            new DeviceEntry(new StringArrayDeviceID(DEVICE_3.split("\\.")), DEVICE_3_ATTRIBUTES),
            new DeviceEntry(new StringArrayDeviceID(DEVICE_2.split("\\.")), DEVICE_2_ATTRIBUTES));
      }
      if (compareEqualsMatch(expressionList.get(0), "tag2", "B2")) {
        return Collections.singletonList(
            new DeviceEntry(new StringArrayDeviceID(DEVICE_5.split("\\.")), DEVICE_5_ATTRIBUTES));
      }
    }

    return Arrays.asList(
        new DeviceEntry(new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_4_ATTRIBUTES),
        new DeviceEntry(new StringArrayDeviceID(DEVICE_1.split("\\.")), DEVICE_1_ATTRIBUTES),
        new DeviceEntry(new StringArrayDeviceID(DEVICE_6.split("\\.")), DEVICE_6_ATTRIBUTES),
        new DeviceEntry(new StringArrayDeviceID(DEVICE_5.split("\\.")), DEVICE_5_ATTRIBUTES),
        new DeviceEntry(new StringArrayDeviceID(DEVICE_3.split("\\.")), DEVICE_3_ATTRIBUTES),
        new DeviceEntry(new StringArrayDeviceID(DEVICE_2.split("\\.")), DEVICE_2_ATTRIBUTES));
  }

  private boolean compareEqualsMatch(Expression expression, String idOrAttr, String value) {
    if (expression instanceof ComparisonExpression
        && ((ComparisonExpression) expression).getOperator()
            == ComparisonExpression.Operator.EQUAL) {
      Expression leftExpression = ((ComparisonExpression) expression).getLeft();
      Expression rightExpression = ((ComparisonExpression) expression).getRight();
      if (leftExpression instanceof SymbolReference && rightExpression instanceof StringLiteral) {
        return ((SymbolReference) leftExpression).getName().equalsIgnoreCase(idOrAttr)
            && ((StringLiteral) rightExpression).getValue().equalsIgnoreCase(value);
      } else if (leftExpression instanceof StringLiteral
          && rightExpression instanceof SymbolReference) {
        return ((SymbolReference) rightExpression).getName().equalsIgnoreCase(idOrAttr)
            && ((StringLiteral) leftExpression).getValue().equalsIgnoreCase(value);
      }
    }

    return false;
  }

  private boolean compareNotEqualsMatch(Expression expression, String idOrAttr, String value) {
    if (expression instanceof ComparisonExpression
        && ((ComparisonExpression) expression).getOperator()
            == ComparisonExpression.Operator.NOT_EQUAL) {
      Expression leftExpression = ((ComparisonExpression) expression).getLeft();
      Expression rightExpression = ((ComparisonExpression) expression).getRight();
      if (leftExpression instanceof SymbolReference && rightExpression instanceof StringLiteral) {
        return ((SymbolReference) leftExpression).getName().equalsIgnoreCase(idOrAttr)
            && ((StringLiteral) rightExpression).getValue().equalsIgnoreCase(value);
      } else if (leftExpression instanceof StringLiteral
          && rightExpression instanceof SymbolReference) {
        return ((SymbolReference) rightExpression).getName().equalsIgnoreCase(idOrAttr)
            && ((StringLiteral) leftExpression).getValue().equalsIgnoreCase(value);
      }
    }

    return false;
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

  private static final DataPartition DATA_PARTITION =
      MockTableModelDataPartition.constructDataPartition();

  private static final SchemaPartition SCHEMA_PARTITION =
      MockTableModelDataPartition.constructSchemaPartition();

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
