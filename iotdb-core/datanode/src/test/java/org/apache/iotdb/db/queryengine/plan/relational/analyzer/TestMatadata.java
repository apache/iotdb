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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
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

import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.getFunctionType;
import static org.apache.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;

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
  private static final ColumnMetadata TIME_CM = new ColumnMetadata(TIME, INT64);
  private static final ColumnMetadata TAG1_CM = new ColumnMetadata(TAG1, BinaryType.TEXT);
  private static final ColumnMetadata TAG2_CM = new ColumnMetadata(TAG2, BinaryType.TEXT);
  private static final ColumnMetadata TAG3_CM = new ColumnMetadata(TAG3, BinaryType.TEXT);
  private static final ColumnMetadata ATTR1_CM = new ColumnMetadata(ATTR1, BinaryType.TEXT);
  private static final ColumnMetadata ATTR2_CM = new ColumnMetadata(ATTR2, BinaryType.TEXT);
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
  public List<DeviceEntry> indexScan(
      QualifiedObjectName tableName,
      List<Expression> expressionList,
      List<String> attributeColumns) {
    return Arrays.asList(
        new DeviceEntry(
            new StringArrayDeviceID("root.testdb", "table1", "t1", "t2", "t3"),
            Arrays.asList("a1", "a2")));
  }

  public static boolean isTwoNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 2
        && isNumericType(argumentTypes.get(0))
        && isNumericType(argumentTypes.get(1));
  }

  public static boolean isOneNumericType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && isNumericType(argumentTypes.get(0));
  }

  public static boolean isOneBooleanType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && BOOLEAN.equals(argumentTypes.get(0));
  }

  public static boolean isOneTextType(List<? extends Type> argumentTypes) {
    return argumentTypes.size() == 1 && TEXT.equals(argumentTypes.get(0));
  }

  public static boolean isNumericType(Type type) {
    return DOUBLE.equals(type) || FLOAT.equals(type) || INT32.equals(type) || INT64.equals(type);
  }

  public static boolean isTwoTypeComparable(List<? extends Type> argumentTypes) {
    if (argumentTypes.size() != 2) {
      return false;
    }
    Type left = argumentTypes.get(0);
    Type right = argumentTypes.get(1);
    if (left.equals(right)) {
      return true;
    }

    // Boolean type and Binary Type can not be compared with other types
    return isNumericType(left) && isNumericType(right);
  }
}
