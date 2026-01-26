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
import org.apache.iotdb.commons.schema.table.InsertNodeMeasurementInfo;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.function.Exclude;
import org.apache.iotdb.db.queryengine.plan.function.Repeat;
import org.apache.iotdb.db.queryengine.plan.function.Split;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.function.TableBuiltinTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.SubtractionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.NonAlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.OperatorNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TreeDeviceViewSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.db.queryengine.plan.udf.BuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.plan.udf.TableUDFUtils;
import org.apache.iotdb.db.schemaengine.table.InformationSchemaUtils;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.udf.api.relational.TableFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.InformationSchema.INFORMATION_DATABASE;
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

public class TestMetadata implements Metadata {

  private final TypeManager typeManager = new InternalTypeManager();

  public static final String DB1 = "testdb";
  public static final String TREE_DB1 = "root.test";
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
  public static final String TABLE3 = "table3";
  private static final String S4 = "s4";
  private static final ColumnMetadata S4_CM = new ColumnMetadata(S4, TIMESTAMP);

  public static final String TREE_VIEW_DB = "tree_view";
  public static final String DEVICE_VIEW_TEST_TABLE = "root.test.device_view";

  @Override
  public boolean tableExists(final QualifiedObjectName name) {
    return name.getDatabaseName().equalsIgnoreCase(DB1)
        && (name.getObjectName().equalsIgnoreCase(TABLE1)
            || name.getObjectName().equalsIgnoreCase(TABLE2)
            || name.getObjectName().equalsIgnoreCase(TABLE3));
  }

  @Override
  public Optional<TableSchema> getTableSchema(SessionInfo session, QualifiedObjectName name) {
    if (name.getDatabaseName().equals(TREE_VIEW_DB)) {
      TreeDeviceViewSchema treeDeviceViewSchema = Mockito.mock(TreeDeviceViewSchema.class);
      Mockito.when(treeDeviceViewSchema.getTableName()).thenReturn(DEVICE_VIEW_TEST_TABLE);
      Mockito.when(treeDeviceViewSchema.getColumns())
          .thenReturn(
              ImmutableList.of(
                  ColumnSchema.builder(TIME_CM)
                      .setColumnCategory(TsTableColumnCategory.TIME)
                      .build(),
                  ColumnSchema.builder(TAG1_CM)
                      .setColumnCategory(TsTableColumnCategory.TAG)
                      .build(),
                  ColumnSchema.builder(TAG2_CM)
                      .setColumnCategory(TsTableColumnCategory.TAG)
                      .build(),
                  ColumnSchema.builder(S1_CM)
                      .setColumnCategory(TsTableColumnCategory.FIELD)
                      .build(),
                  ColumnSchema.builder(S2_CM)
                      .setColumnCategory(TsTableColumnCategory.FIELD)
                      .build()));
      Mockito.when(treeDeviceViewSchema.getColumn2OriginalNameMap())
          .thenReturn(ImmutableMap.of(TAG1, "province", TAG2, "city"));
      return Optional.of(treeDeviceViewSchema);
    }

    if (name.getDatabaseName().equals(INFORMATION_DATABASE)) {
      TsTable table =
          InformationSchemaUtils.mayGetTable(
              INFORMATION_DATABASE, name.getObjectName().toLowerCase(Locale.ENGLISH));
      if (table == null) {
        return Optional.empty();
      }
      final List<ColumnSchema> columnSchemaList =
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

    if (name.getObjectName().equalsIgnoreCase(TABLE1)) {
      final List<ColumnSchema> columnSchemas =
          Arrays.asList(
              ColumnSchema.builder(TIME_CM).setColumnCategory(TsTableColumnCategory.TIME).build(),
              ColumnSchema.builder(TAG1_CM).setColumnCategory(TsTableColumnCategory.TAG).build(),
              ColumnSchema.builder(TAG2_CM).setColumnCategory(TsTableColumnCategory.TAG).build(),
              ColumnSchema.builder(TAG3_CM).setColumnCategory(TsTableColumnCategory.TAG).build(),
              ColumnSchema.builder(ATTR1_CM)
                  .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
                  .build(),
              ColumnSchema.builder(ATTR2_CM)
                  .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
                  .build(),
              ColumnSchema.builder(S1_CM).setColumnCategory(TsTableColumnCategory.FIELD).build(),
              ColumnSchema.builder(S2_CM).setColumnCategory(TsTableColumnCategory.FIELD).build(),
              ColumnSchema.builder(S3_CM).setColumnCategory(TsTableColumnCategory.FIELD).build());

      return Optional.of(new TableSchema(TABLE1, columnSchemas));
    } else {
      List<ColumnSchema> columnSchemas =
          Arrays.asList(
              ColumnSchema.builder(TIME_CM).setColumnCategory(TsTableColumnCategory.TIME).build(),
              ColumnSchema.builder(TAG1_CM).setColumnCategory(TsTableColumnCategory.TAG).build(),
              ColumnSchema.builder(TAG2_CM).setColumnCategory(TsTableColumnCategory.TAG).build(),
              ColumnSchema.builder(TAG3_CM).setColumnCategory(TsTableColumnCategory.TAG).build(),
              ColumnSchema.builder(ATTR1_CM)
                  .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
                  .build(),
              ColumnSchema.builder(ATTR2_CM)
                  .setColumnCategory(TsTableColumnCategory.ATTRIBUTE)
                  .build(),
              ColumnSchema.builder(S1_CM).setColumnCategory(TsTableColumnCategory.FIELD).build(),
              ColumnSchema.builder(S2_CM).setColumnCategory(TsTableColumnCategory.FIELD).build(),
              ColumnSchema.builder(S3_CM).setColumnCategory(TsTableColumnCategory.FIELD).build(),
              ColumnSchema.builder(S4_CM).setColumnCategory(TsTableColumnCategory.FIELD).build());

      return Optional.of(new TableSchema(name.getObjectName(), columnSchemas));
    }
  }

  @Override
  public Type getOperatorReturnType(
      final OperatorType operatorType, final List<? extends Type> argumentTypes)
      throws OperatorNotFoundException {

    switch (operatorType) {
      case SUBTRACT:
        Optional<Type> resolvedType = SubtractionResolver.checkConditions(argumentTypes);
        return resolvedType.orElseThrow(
            () ->
                new OperatorNotFoundException(
                    operatorType,
                    argumentTypes,
                    new IllegalArgumentException(
                        "The combination of argument types is not supported for this operator.")));
      case ADD:
      case MULTIPLY:
      case DIVIDE:
      case MODULUS:
        if (!isTwoNumericType(argumentTypes)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Should have two numeric operands."));
        }
        if (argumentTypes.get(0).equals(TIMESTAMP) && argumentTypes.get(1).equals(TIMESTAMP)) {
          throw new OperatorNotFoundException(
              operatorType,
              argumentTypes,
              new IllegalArgumentException("Cannot apply operator: TIMESTAMP - TIMESTAMP"));
        }
        if (argumentTypes.get(0).equals(TIMESTAMP) || argumentTypes.get(1).equals(TIMESTAMP)) {
          return TIMESTAMP;
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
  public Type getFunctionReturnType(
      final String functionName, final List<? extends Type> argumentTypes) {
    return getFunctionType(functionName, argumentTypes);
  }

  @Override
  public boolean isAggregationFunction(
      final SessionInfo session, final String functionName, final AccessControl accessControl) {
    return BuiltinAggregationFunction.getNativeFunctionNames()
        .contains(functionName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Type getType(final TypeSignature signature) throws TypeNotFoundException {
    return typeManager.getType(signature);
  }

  @Override
  public boolean canCoerce(final Type from, final Type to) {
    return true;
  }

  @Override
  public IPartitionFetcher getPartitionFetcher() {
    return getFakePartitionFetcher();
  }

  @Override
  public Map<String, List<DeviceEntry>> indexScan(
      final QualifiedObjectName tableName,
      final List<Expression> expressionList,
      final List<String> attributeColumns,
      final MPPQueryContext context) {
    if (tableName.getDatabaseName().equals(TREE_VIEW_DB)) {
      if (expressionList.isEmpty()) {
        return Collections.singletonMap(
            TREE_DB1,
            ImmutableList.of(
                new AlignedDeviceEntry(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_3), new Binary[0]),
                new AlignedDeviceEntry(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_6), new Binary[0]),
                new NonAlignedDeviceEntry(
                    IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_5), new Binary[0])));
      }

      return Collections.singletonMap(
          TREE_DB1,
          ImmutableList.of(
              new AlignedDeviceEntry(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_3), new Binary[0]),
              new AlignedDeviceEntry(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(DEVICE_6), new Binary[0])));
    }

    if (expressionList.size() == 3) {
      if (compareEqualsMatch(expressionList.get(0), "tag1", "shanghai")
          && compareEqualsMatch(expressionList.get(1), "tag2", "A3")
          && compareEqualsMatch(expressionList.get(2), "tag3", "YY")) {
        return Collections.singletonMap(
            DB1,
            Collections.singletonList(
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_3.split("\\.")), DEVICE_1_ATTRIBUTES)));
      }
    } else if (expressionList.size() == 2) {
      if (compareEqualsMatch(expressionList.get(0), "tag1", "beijing")
              && compareEqualsMatch(expressionList.get(1), "tag2", "A1")
          || compareEqualsMatch(expressionList.get(1), "tag1", "beijing")
              && compareEqualsMatch(expressionList.get(0), "tag2", "A1")) {
        return Collections.singletonMap(
            DB1,
            Collections.singletonList(
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_1.split("\\.")), DEVICE_1_ATTRIBUTES)));
      }
      if (compareEqualsMatch(expressionList.get(0), "tag1", "shanghai")
              && compareEqualsMatch(expressionList.get(1), "tag2", "B3")
          || compareEqualsMatch(expressionList.get(1), "tag1", "shanghai")
              && compareEqualsMatch(expressionList.get(0), "tag2", "B3")) {
        return Collections.singletonMap(
            DB1,
            Collections.singletonList(
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_1_ATTRIBUTES)));
      }

    } else if (expressionList.size() == 1) {
      if (compareEqualsMatch(expressionList.get(0), "tag1", "shanghai")) {
        return Collections.singletonMap(
            DB1,
            Arrays.asList(
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_4_ATTRIBUTES),
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_3.split("\\.")), DEVICE_3_ATTRIBUTES)));
      }
      if (compareEqualsMatch(expressionList.get(0), "tag1", "shenzhen")) {
        return Collections.singletonMap(
            DB1,
            Arrays.asList(
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_6.split("\\.")), DEVICE_6_ATTRIBUTES),
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_5.split("\\.")), DEVICE_5_ATTRIBUTES)));
      }
      if (compareNotEqualsMatch(expressionList.get(0), "tag1", "shenzhen")) {
        return Collections.singletonMap(
            DB1,
            Arrays.asList(
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_4_ATTRIBUTES),
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_1.split("\\.")), DEVICE_1_ATTRIBUTES),
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_3.split("\\.")), DEVICE_3_ATTRIBUTES),
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_2.split("\\.")), DEVICE_2_ATTRIBUTES)));
      }
      if (compareEqualsMatch(expressionList.get(0), "tag2", "B2")) {
        return Collections.singletonMap(
            DB1,
            Collections.singletonList(
                new AlignedDeviceEntry(
                    new StringArrayDeviceID(DEVICE_5.split("\\.")), DEVICE_5_ATTRIBUTES)));
      }
    }

    return Collections.singletonMap(
        DB1,
        Arrays.asList(
            new AlignedDeviceEntry(
                new StringArrayDeviceID(DEVICE_4.split("\\.")), DEVICE_4_ATTRIBUTES),
            new AlignedDeviceEntry(
                new StringArrayDeviceID(DEVICE_1.split("\\.")), DEVICE_1_ATTRIBUTES),
            new AlignedDeviceEntry(
                new StringArrayDeviceID(DEVICE_6.split("\\.")), DEVICE_6_ATTRIBUTES),
            new AlignedDeviceEntry(
                new StringArrayDeviceID(DEVICE_5.split("\\.")), DEVICE_5_ATTRIBUTES),
            new AlignedDeviceEntry(
                new StringArrayDeviceID(DEVICE_3.split("\\.")), DEVICE_3_ATTRIBUTES),
            new AlignedDeviceEntry(
                new StringArrayDeviceID(DEVICE_2.split("\\.")), DEVICE_2_ATTRIBUTES)));
  }

  private boolean compareEqualsMatch(
      final Expression expression, final String idOrAttr, final String value) {
    if (expression instanceof ComparisonExpression
        && ((ComparisonExpression) expression).getOperator()
            == ComparisonExpression.Operator.EQUAL) {
      final Expression leftExpression = ((ComparisonExpression) expression).getLeft();
      final Expression rightExpression = ((ComparisonExpression) expression).getRight();
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

  private boolean compareNotEqualsMatch(
      final Expression expression, final String idOrAttr, final String value) {
    if (expression instanceof ComparisonExpression
        && ((ComparisonExpression) expression).getOperator()
            == ComparisonExpression.Operator.NOT_EQUAL) {
      final Expression leftExpression = ((ComparisonExpression) expression).getLeft();
      final Expression rightExpression = ((ComparisonExpression) expression).getRight();
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
      final String database,
      final TableSchema tableSchema,
      final MPPQueryContext context,
      final boolean allowCreateTable,
      final boolean isStrictIdColumn) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateInsertNodeMeasurements(
      final String database,
      final InsertNodeMeasurementInfo measurementInfo,
      final MPPQueryContext context,
      final boolean allowCreateTable,
      final TableHeaderSchemaValidator.MeasurementValidator measurementValidator,
      final TableHeaderSchemaValidator.TagColumnHandler tagColumnHandler) {

    throw new UnsupportedOperationException();
  }

  @Override
  public void validateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation, final MPPQueryContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaPartition getOrCreateSchemaPartition(
      final String database, final List<IDeviceID> deviceIDList, final String userName) {
    return null;
  }

  @Override
  public SchemaPartition getSchemaPartition(
      final String database, final List<IDeviceID> deviceIDList) {
    return null;
  }

  @Override
  public SchemaPartition getSchemaPartition(final String database) {
    return null;
  }

  @Override
  public DataPartition getDataPartition(
      final String database, final List<DataPartitionQueryParam> sgNameToQueryParamsMap) {
    return TREE_DB1.equals(database) ? TREE_VIEW_DATA_PARTITION : TABLE_DATA_PARTITION;
  }

  @Override
  public DataPartition getDataPartitionWithUnclosedTimeRange(
      final String database, final List<DataPartitionQueryParam> sgNameToQueryParamsMap) {
    return TREE_DB1.equals(database) ? TREE_VIEW_DATA_PARTITION : TABLE_DATA_PARTITION;
  }

  @Override
  public TableFunction getTableFunction(String functionName) {
    if ("EXCLUDE".equalsIgnoreCase(functionName)) {
      return new Exclude();
    } else if ("REPEAT".equalsIgnoreCase(functionName)) {
      return new Repeat();
    } else if ("SPLIT".equalsIgnoreCase(functionName)) {
      return new Split();
    } else {
      if (TableBuiltinTableFunction.isBuiltInTableFunction(functionName)) {
        return TableBuiltinTableFunction.getBuiltinTableFunction(functionName);
      } else if (TableUDFUtils.isTableFunction(functionName)) {
        return TableUDFUtils.getTableFunction(functionName);
      } else {
        throw new SemanticException("Unknown function: " + functionName);
      }
    }
  }

  private static final DataPartition TABLE_DATA_PARTITION =
      MockTableModelDataPartition.constructDataPartition(DB1);

  private static final DataPartition TREE_VIEW_DATA_PARTITION =
      MockTableModelDataPartition.constructDataPartition(TREE_DB1);

  private static final SchemaPartition TABLE_SCHEMA_PARTITION =
      MockTableModelDataPartition.constructSchemaPartition(DB1);

  private static final SchemaPartition TREE_SCHEMA_PARTITION =
      MockTableModelDataPartition.constructSchemaPartition(TREE_DB1);

  private static IPartitionFetcher getFakePartitionFetcher() {

    return new IPartitionFetcher() {

      @Override
      public SchemaPartition getSchemaPartition(PathPatternTree patternTree) {
        return TABLE_SCHEMA_PARTITION;
      }

      @Override
      public SchemaPartition getSchemaPartition(PathPatternTree patternTree, boolean needAuditDB) {
        return TABLE_SCHEMA_PARTITION;
      }

      @Override
      public SchemaPartition getOrCreateSchemaPartition(
          PathPatternTree patternTree, String userName) {
        return TABLE_SCHEMA_PARTITION;
      }

      @Override
      public DataPartition getDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return !sgNameToQueryParamsMap.isEmpty() && sgNameToQueryParamsMap.get(TREE_VIEW_DB) != null
            ? TREE_VIEW_DATA_PARTITION
            : TABLE_DATA_PARTITION;
      }

      @Override
      public DataPartition getDataPartitionWithUnclosedTimeRange(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return !sgNameToQueryParamsMap.isEmpty() && sgNameToQueryParamsMap.get(TREE_VIEW_DB) != null
            ? TREE_VIEW_DATA_PARTITION
            : TABLE_DATA_PARTITION;
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap) {
        return TABLE_DATA_PARTITION;
      }

      @Override
      public DataPartition getOrCreateDataPartition(
          List<DataPartitionQueryParam> dataPartitionQueryParams, String userName) {
        return TABLE_DATA_PARTITION;
      }

      @Override
      public SchemaNodeManagementPartition getSchemaNodeManagementPartitionWithLevel(
          PathPatternTree patternTree,
          PathPatternTree scope,
          Integer level,
          boolean canSeeAuditDB) {
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
        return TREE_VIEW_DB.equals(database) ? TREE_SCHEMA_PARTITION : TABLE_SCHEMA_PARTITION;
      }

      @Override
      public SchemaPartition getSchemaPartition(String database, List<IDeviceID> deviceIDList) {
        return TREE_VIEW_DB.equals(database) ? TREE_SCHEMA_PARTITION : TABLE_SCHEMA_PARTITION;
      }
    };
  }
}
