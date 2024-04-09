package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
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
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignature;
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.DoubleType;
import org.apache.iotdb.tsfile.read.common.type.Type;

import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.tsfile.read.common.type.IntType.INT32;
import static org.apache.iotdb.tsfile.read.common.type.LongType.INT64;

public class TestMatadata implements Metadata {

  public static final String DB1 = "db1";
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
  private static final ColumnMetadata S1_CM = new ColumnMetadata(S1, INT32);
  private static final ColumnMetadata S2_CM = new ColumnMetadata(S2, DoubleType.DOUBLE);

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
    return Optional.empty();
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
    return null;
  }

  @Override
  public Type getOperatorReturnType(OperatorType operatorType, List<? extends Type> argumentTypes)
      throws OperatorNotFoundException {
    return null;
  }

  @Override
  public Type getFunctionReturnType(String functionName, List<? extends Type> argumentTypes) {
    return null;
  }

  @Override
  public boolean isAggregationFunction(
      SessionInfo session, String functionName, AccessControl accessControl) {
    return false;
  }

  @Override
  public Type getType(TypeSignature signature) throws TypeNotFoundException {
    return null;
  }

  @Override
  public boolean canCoerce(Type from, Type to) {
    return false;
  }
}
