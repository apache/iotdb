package org.apache.iotdb.db.query.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.ExplicitReturnTypeInference;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.StorageGroupManager;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.MTree;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.executor.calcite.IoTDBTable;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

public class IoTDBCalciteSchema extends CalciteSchema {

//  static class IoTDBTable implements ScannableTable, FilterableTable, Table {
//
//    private final String path;
//    private final TSDataType dataType;
//    private final String name;
//
//    public IoTDBTable(String path) {
//      this.path = path;
//      try {
//        IMeasurementSchema schema = MManager.getInstance().getMeasurementMNode(new PartialPath(this.path)).getSchema();
//        name = schema.getMeasurementId();
//        dataType = schema.getType();
//      } catch (MetadataException e) {
//        throw new IllegalArgumentException("Unable to find DataType for Path " + path);
//      }
//    }
//
//    @Override
//    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//      return typeFactory.createStructType(
//          Arrays.asList(
//              typeFactory.createJavaType(Long.class),
//              typeFactory.createJavaType(getClassForDataType(this.dataType))
//          ),
//          Arrays.asList(
//              "time",
//              name
//          )
//      );
//    }
//
//    private Class<?> getClassForDataType(TSDataType type) {
//      switch (type) {
//        case INT32:
//          return Integer.class;
//        case TEXT:
//          return String.class;
//        default:
//          throw new NotImplementedException("Type " + type + " not yet supported!");
//      }
//    }
//
//    @Override
//    public Statistic getStatistic() {
//      return Statistics.of(null, null, null, Arrays.asList(RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING))));
//    }
//
//    @Override
//    public Schema.TableType getJdbcTableType() {
//      return null;
//    }
//
//    @Override
//    public boolean isRolledUp(String column) {
//      return false;
//    }
//
//    @Override
//    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
//      return false;
//    }
//
//    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
//      throw new UnsupportedOperationException();
//    }
//
//    public Type getElementType() {
//      // Should be allowed as we return Object[]
//      return null;
//    }
//
//    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
//      Method apply;
//      try {
//        apply = Function2.class.getMethod("apply", Object.class, Object.class);
//      } catch (NoSuchMethodException e) {
//        throw new IllegalStateException();
//      }
//      return Expressions.convert_(
//          Expressions.call(
//              Expressions.convert_(
//                  Expressions.call(
//                      DataContext.ROOT,
//                      "get",
//                      Expressions.constant("series", String.class)
//                  ),
//                  Function2.class
//              ),
//              apply,
//              Expressions.constant(path, String.class),
//              Expressions.constant(dataType, TSDataType.class)
//          ),
//          Enumerable.class
//      );
//    }
//
//    @Override
//    public Enumerable<Object[]> scan(DataContext root) {
//      Enumerable<Object[]> enumerable = (Enumerable<Object[]>) ((Function2) root.get("series")).apply(path, dataType);
////      Enumerator<Object[]> enumerator = enumerable.enumerator();
////
////      while (enumerator.moveNext()) {
////        Object[] current = enumerator.current();
////
////        System.out.println(" ==> " + Arrays.toString(current));
////      }
//      return enumerable;
//    }
//
//    @Override
//    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
//      return scan(root);
//    }
//
////    @Override
////    public Enumerable<Object[]> scan(DataContext root) {
////      return Linq4j.asEnumerable(Arrays.asList(
////          new Object[]{1L, 1},
////          new Object[]{2L, 3}
////      ));
////    }
//  }

  protected IoTDBCalciteSchema(@Nullable CalciteSchema parent, Schema schema, String name) {
    super(parent, schema, name, null, null, null, null, null, null, null, null);
  }

  @Override
  protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
    if ("root".equals(schemaName)) {
      return new IoTDBCalciteSchema(this, schema, "root");
    }
    throw new UnsupportedOperationException();
  }

  @Override
  protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
    return new TableEntryImpl(this, tableName, new IoTDBTable(tableName), ImmutableList.of());
  }

  @Override
  protected @Nullable TypeEntry getImplicitType(String name, boolean caseSensitive) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(String tableName, boolean caseSensitive) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addImplicitSubSchemaToBuilder(ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addImplicitFunctionsToBuilder(ImmutableList.Builder<Function> builder, String name, boolean caseSensitive) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(ImmutableSortedMap.Builder<String, Table> builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected CalciteSchema snapshot(@Nullable CalciteSchema parent, SchemaVersion version) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean isCacheEnabled() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCache(boolean cache) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CalciteSchema add(String name, Schema schema) {
    throw new UnsupportedOperationException();
  }
}
