package org.apache.iotdb.db.query.executor.calcite;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.QueryableDefaults;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.calcite.schema.TranslatableTable;


import java.util.Arrays;

public class IoTDBTable extends AbstractQueryableTable implements TranslatableTable {

  private final String path;
  private final TSDataType dataType;
  private final String name;

  public IoTDBTable(String path) {
    super(Object[].class);
    this.path = path;
    try {
      IMeasurementSchema schema = MManager.getInstance().getMeasurementMNode(new PartialPath(this.path)).getSchema();
      name = schema.getMeasurementId();
      dataType = schema.getType();
    } catch (MetadataException e) {
      throw new IllegalArgumentException("Unable to find DataType for Path " + path);
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(
        Arrays.asList(
            typeFactory.createJavaType(Long.class),
            typeFactory.createJavaType(getClassForDataType(this.dataType))
        ),
        Arrays.asList(
            "time",
            name
        )
    );
  }

  private Class<?> getClassForDataType(TSDataType type) {
    switch (type) {
      case INT32:
        return Integer.class;
      case TEXT:
        return String.class;
      default:
        throw new NotImplementedException("Type " + type + " not yet supported!");
    }
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.of(null, null, null, Arrays.asList(RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING))));
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return null;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
    return false;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new IoTDBTableScan(cluster, cluster.traitSetOf(IoTDBRel.CONVENTION),
        relOptTable, this, null);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new IoTDBQueryable<>(queryProvider, schema, this, tableName);
  }

  public static class IoTDBQueryable<T> extends AbstractTableQueryable<T> {
    IoTDBQueryable(QueryProvider queryProvider, SchemaPlus schema,
                   IoTDBTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    @Override public Enumerator<T> enumerator() {
      //noinspection unchecked
      // TODO
//      final Enumerable<T> enumerable =
//          (Enumerable<T>) getTable().find(getMongoDb(), null, null, null);
//      return enumerable.enumerator();
      return (Enumerator<T>) Linq4j.asEnumerable(
          new Object[]{
              new Object[]{16L, 109},
              new Object[]{20L, 129}
          }
      ).enumerator();
    }

//    private MongoDatabase getMongoDb() {
//      return schema.unwrap(MongoSchema.class).mongoDb;
//    }
//
//    private MongoTable getTable() {
//      return (MongoTable) table;
//    }
//
//    /** Called via code-generation.
//     *
//     * @see org.apache.calcite.adapter.mongodb.MongoMethod#MONGO_QUERYABLE_AGGREGATE
//     */
//    @SuppressWarnings("UnusedDeclaration")
//    public Enumerable<Object> aggregate(List<Map.Entry<String, Class>> fields,
//                                        List<String> operations) {
//      return getTable().aggregate(getMongoDb(), fields, operations);
//    }
//
//    /** Called via code-generation.
//     *
//     * @param filterJson Filter document
//     * @param projectJson Projection document
//     * @param fields List of expected fields (and their types)
//     * @return result of mongo query
//     *
//     * @see org.apache.calcite.adapter.mongodb.MongoMethod#MONGO_QUERYABLE_FIND
//     */
//    @SuppressWarnings("UnusedDeclaration")
//    public Enumerable<Object> find(String filterJson,
//                                   String projectJson, List<Map.Entry<String, Class>> fields) {
//      return getTable().find(getMongoDb(), filterJson, projectJson, fields);
//    }
  }
}
