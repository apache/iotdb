package org.apache.iotdb.db.query.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Calcite based Execution
 */
public class CalciteIoTDBSchema extends CalciteSchema{

  protected CalciteIoTDBSchema(@Nullable CalciteSchema parent, Schema schema, String name, @Nullable NameMap<CalciteSchema> subSchemaMap, @Nullable NameMap<TableEntry> tableMap, @Nullable NameMap<LatticeEntry> latticeMap, @Nullable NameMap<TypeEntry> typeMap, @Nullable NameMultimap<FunctionEntry> functionMap, @Nullable NameSet functionNames, @Nullable NameMap<FunctionEntry> nullaryFunctionMap, @Nullable List<? extends List<String>> path) {
    super(parent, schema, name, subSchemaMap, tableMap, latticeMap, typeMap, functionMap, functionNames, nullaryFunctionMap, path);
  }

  @Override
  protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
    throw new UnsupportedOperationException();
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
