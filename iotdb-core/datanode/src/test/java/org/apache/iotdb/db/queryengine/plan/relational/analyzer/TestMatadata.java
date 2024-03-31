package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.function.OperatorType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnHandle;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.OperatorNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableHandle;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
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
    public static final String DB2 = "db2";
    public static final String TABLE1 = "table1";
    public static final String TABLE2 = "table2";


    @Override
    public boolean tableExists(QualifiedObjectName name) {
        return name.getDatabaseName().equalsIgnoreCase(DB1) && name.getObjectName().equalsIgnoreCase(TABLE1);
    }

    @Override
    public TableSchema getTableSchema(SessionInfo session, TableHandle tableHandle) {
        return null;
    }

    @Override
    public TableMetadata getTableMetadata(SessionInfo session, TableHandle tableHandle) {
        return null;
    }

    @Override
    public Optional<TableHandle> getTableHandle(SessionInfo session, QualifiedObjectName name) {
        return Optional.empty();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(SessionInfo session, TableHandle tableHandle) {
        Map<String, ColumnHandle> map = new HashMap<>();
        TableSchema tableSchema = Mockito.mock(TableSchema.class);
        Mockito.when(tableSchema.getTableName()).thenReturn("table1");
        ColumnSchema column1 =
                ColumnSchema.builder().setName("time").setType(INT64).setHidden(false).build();
        ColumnHandle column1Handle = Mockito.mock(ColumnHandle.class);
        map.put("time", column1Handle);
        ColumnSchema column2 =
                ColumnSchema.builder().setName("s1").setType(INT32).setHidden(false).build();
        ColumnHandle column2Handle = Mockito.mock(ColumnHandle.class);
        map.put("s1", column2Handle);
        ColumnSchema column3 =
                ColumnSchema.builder().setName("s2").setType(INT64).setHidden(false).build();
        ColumnHandle column3Handle = Mockito.mock(ColumnHandle.class);
        map.put("s2", column3Handle);
        List<ColumnSchema> columnSchemaList = Arrays.asList(column1, column2, column3);
        return columnSchemaList;
    }

    @Override
    public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes) throws OperatorNotFoundException {
        return null;
    }
}
