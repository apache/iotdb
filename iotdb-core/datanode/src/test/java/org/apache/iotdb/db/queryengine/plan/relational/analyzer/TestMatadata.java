package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinScalarFunction;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionId;
import org.apache.iotdb.db.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnHandle;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.OperatorNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableHandle;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.BooleanType;
import org.apache.iotdb.tsfile.read.common.type.Type;

import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.tsfile.read.common.type.BinaryType.TEXT;
import static org.apache.iotdb.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.iotdb.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.iotdb.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.iotdb.tsfile.read.common.type.IntType.INT32;
import static org.apache.iotdb.tsfile.read.common.type.LongType.INT64;

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
  private static final ColumnMetadata TIME_CM = new ColumnMetadata(TIME, INT64);
  private static final ColumnMetadata TAG1_CM = new ColumnMetadata(TAG1, BinaryType.TEXT);
  private static final ColumnMetadata TAG2_CM = new ColumnMetadata(TAG2, BinaryType.TEXT);
  private static final ColumnMetadata TAG3_CM = new ColumnMetadata(TAG3, BinaryType.TEXT);
  private static final ColumnMetadata ATTR1_CM = new ColumnMetadata(ATTR1, BinaryType.TEXT);
  private static final ColumnMetadata ATTR2_CM = new ColumnMetadata(ATTR2, BinaryType.TEXT);
  private static final ColumnMetadata S1_CM = new ColumnMetadata(S1, INT64);
  private static final ColumnMetadata S2_CM = new ColumnMetadata(S2, INT64);

  public static final String DB2 = "db2";
  public static final String TABLE2 = "table2";

  @Override
  public boolean tableExists(QualifiedObjectName name) {
    return name.getDatabaseName().equalsIgnoreCase(DB1)
        && name.getObjectName().equalsIgnoreCase(TABLE1);
  }

  @Override
  public TableSchema getTableSchema(SessionInfo session, TableHandle tableHandle) {
    List<ColumnSchema> columnSchemas =
        Arrays.asList(
            ColumnSchema.builder(TIME_CM).build(),
            ColumnSchema.builder(TAG1_CM).build(),
            ColumnSchema.builder(TAG2_CM).build(),
            ColumnSchema.builder(TAG3_CM).build(),
            ColumnSchema.builder(ATTR1_CM).build(),
            ColumnSchema.builder(ATTR2_CM).build(),
            ColumnSchema.builder(S1_CM).build(),
            ColumnSchema.builder(S2_CM).build());

    return new TableSchema(TABLE1, columnSchemas);
  }

  @Override
  public TableMetadata getTableMetadata(SessionInfo session, TableHandle tableHandle) {
    return new TableMetadata(
        TABLE1,
        Arrays.asList(TIME_CM, TAG1_CM, TAG2_CM, TAG3_CM, ATTR1_CM, ATTR2_CM, S1_CM, S2_CM));
  }

  @Override
  public Optional<TableHandle> getTableHandle(SessionInfo session, QualifiedObjectName name) {
    return Optional.of(Mockito.mock(TableHandle.class));
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(SessionInfo session, TableHandle tableHandle) {
    Map<String, ColumnHandle> map = new HashMap<>();
    ColumnHandle columnHandle = Mockito.mock(ColumnHandle.class);
    map.put(TIME, columnHandle);
    map.put(TAG1, columnHandle);
    map.put(TAG2, columnHandle);
    map.put(TAG3, columnHandle);
    map.put(ATTR1, columnHandle);
    map.put(ATTR2, columnHandle);
    map.put(S1, columnHandle);
    map.put(S2, columnHandle);
    return map;
  }

  @Override
  public ResolvedFunction resolveOperator(
      OperatorType operatorType, List<? extends Type> argumentTypes)
      throws OperatorNotFoundException {
    if (operatorType == OperatorType.LESS_THAN) {
      return new ResolvedFunction(
          new BoundSignature("less_than", BooleanType.BOOLEAN, Arrays.asList(INT64, INT32)),
          new FunctionId("less_than"),
          FunctionKind.SCALAR,
          true);
    } else if (operatorType == OperatorType.ADD) {
      return new ResolvedFunction(
          new BoundSignature("add", INT64, Arrays.asList(INT64, INT32)),
          new FunctionId("add"),
          FunctionKind.SCALAR,
          true);
    } else if (operatorType == OperatorType.EQUAL) {
      return new ResolvedFunction(
          new BoundSignature(
              "equals", BooleanType.BOOLEAN, Arrays.asList(BinaryType.TEXT, BinaryType.TEXT)),
          new FunctionId("equals"),
          FunctionKind.SCALAR,
          true);
    } else {
      throw new OperatorNotFoundException(operatorType, argumentTypes, new RuntimeException());
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

    // builtin scalar function
    if (BuiltinScalarFunction.DIFF.getFunctionName().equalsIgnoreCase(functionName)
        || BuiltinScalarFunction.ROUND.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isOneNumericType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function"
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports numeric data types [INT32, INT64, FLOAT, DOUBLE]");
      }
      return DOUBLE;
    } else if (BuiltinScalarFunction.REPLACE.getFunctionName().equalsIgnoreCase(functionName)
        || BuiltinScalarFunction.SUBSTRING.getFunctionName().equalsIgnoreCase(functionName)) {
      if (!isOneTextType(argumentTypes)) {
        throw new SemanticException(
            "Scalar function"
                + functionName.toLowerCase(Locale.ENGLISH)
                + " only supports text data type.");
      }
      return TEXT;
    }

    // builtin aggregation function
    // check argument type
    switch (functionName.toLowerCase()) {
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.EXTREME:
      case SqlConstant.MIN_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        if (!isOneNumericType(argumentTypes)) {
          throw new SemanticException(
              String.format(
                  "Aggregate functions [%s] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]",
                  functionName));
        }
        break;
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.TIME_DURATION:
      case SqlConstant.MODE:
        if (argumentTypes.size() != 1) {
          throw new SemanticException(
              String.format(
                  "Aggregate functions [%s] should only have one argument", functionName));
        }
        break;
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        if (argumentTypes.size() != 2) {
          throw new SemanticException(
              String.format(
                  "Aggregate functions [%s] should only have two arguments", functionName));
        } else if (!argumentTypes.get(1).isOrderable()) {
          throw new SemanticException(
              String.format(
                  "Second argument of Aggregate functions [%s] should be orderable", functionName));
        }

        break;
      case SqlConstant.COUNT:
        break;
      default:
        // ignore
    }

    // get return type
    switch (functionName.toLowerCase()) {
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.COUNT:
      case SqlConstant.TIME_DURATION:
        return INT64;
      case SqlConstant.MIN_VALUE:
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.EXTREME:
      case SqlConstant.MODE:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
        return argumentTypes.get(0);
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        return DOUBLE;
      default:
        // ignore
    }

    // TODO scalar UDF function

    // TODO UDAF

    throw new SemanticException("Unknown function: " + functionName);
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
