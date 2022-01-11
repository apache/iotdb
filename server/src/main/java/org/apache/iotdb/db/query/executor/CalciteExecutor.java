package org.apache.iotdb.db.query.executor;

import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.iterator.SeriesIterator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Calcite based Execution
 */
public class CalciteExecutor {

  private final JavaTypeFactoryImpl typeFactory;
  private final RexBuilder rexBuilder;
  private final CalciteSchema rootSchema;
  private final CalciteConnectionConfigImpl calciteConnectionConfig;
  private final CalciteCatalogReader catalogReader;
  private final RelBuilder relBuilder;
  private final RelOptCluster cluster;
  private final RelOptPlanner planner;

  public CalciteExecutor() {
    typeFactory = new JavaTypeFactoryImpl();
    rexBuilder = new RexBuilder(typeFactory);
//    rootSchema = new CalciteIoTDBSchema(null, null, null, null, null, null, null, null, null, null, null);

    // Register all here
//    rootSchema.add("d1.s1", new SeriesScan("d1.s1", TSDataType.INT32));
//    rootSchema.add("d1.s2", new SeriesScan("d1.s2", TSDataType.INT32));

    Schema testSchema = new Schema() {
      @Override
      public @Nullable Table getTable(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<String> getTableNames() {
        throw new UnsupportedOperationException();
      }

      @Override
      public @Nullable RelProtoDataType getType(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<String> getTypeNames() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Collection<Function> getFunctions(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<String> getFunctionNames() {
        throw new UnsupportedOperationException();
      }

      @Override
      public @Nullable Schema getSubSchema(String name) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<String> getSubSchemaNames() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
        // Copied from AbstractSchema
        MethodCallExpression rootSchema = Expressions.call(
            DataContext.ROOT,
            BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
        Expression call =
            Expressions.call(
                rootSchema,
                BuiltInMethod.SCHEMA_GET_SUB_SCHEMA.method,
                Expressions.constant(name));
        return call;
      }

      @Override
      public boolean isMutable() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Schema snapshot(SchemaVersion version) {
        throw new UnsupportedOperationException();
      }
    };

    rootSchema = new IoTDBCalciteSchema(null, testSchema, "root");

    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .build();

    catalogReader = new CalciteCatalogReader(rootSchema, Collections.singletonList("root"), typeFactory, null);
//    relBuilder = RelBuilder.create(frameworkConfig);

    planner = new VolcanoPlanner();

    RelOptUtil.registerDefaultRules(planner, false, true);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    cluster = RelOptCluster.create(planner, rexBuilder);
    relBuilder = new IoTDBRelBuilder(frameworkConfig.getContext(), cluster, catalogReader);

//    catalogReader.getSchemaPaths().add(Collections.singletonList("root"));
    // ...
    calciteConnectionConfig = new CalciteConnectionConfigImpl(new Properties());

  }

  public QueryDataSet execute(QueryContext queryContext, RelNode root) {
    RelTraitSet desired = cluster.traitSet()
        .replace(BindableConvention.INSTANCE)
        .replace(RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)));

    RelNode expectedRoot = planner.changeTraits(root, desired);
    planner.setRoot(expectedRoot);

    Hook.JAVA_PLAN.addThread((Consumer<? extends Object>) System.out::println);

    RelNode exp = planner.findBestExp();
    Bindable bestExp = (Bindable) exp;

    System.out.println(RelOptUtil.toString(root, SqlExplainLevel.ALL_ATTRIBUTES));
    System.out.println(RelOptUtil.toString(exp, SqlExplainLevel.ALL_ATTRIBUTES));


    Enumerable<@Nullable Object[]> enumerable = bestExp.bind(this.getContext(queryContext));

    if (!exp.getRowType().isStruct()) {
      throw new NotImplementedException();
    }

    TSDataType[] dataTypes = exp.getRowType().getFieldList().stream().filter(
        field -> !Arrays.asList("time", "$f0").contains(field.getName())
    ).map(field -> calciteTypeToTSDataType(field.getType())).toArray(TSDataType[]::new);

    return new EnumeratorDataSet(dataTypes, enumerable.enumerator());
  }

  private TSDataType calciteTypeToTSDataType(RelDataType type) {
    switch (type.getFullTypeString()) {
      case "JavaType(class java.lang.Integer)":
        return TSDataType.INT32;
      case "JavaType(class java.lang.Long)":
      case "JavaType(long) NOT NULL":
        return TSDataType.INT64;
      case "JavaType(class java.lang.String)":
        return TSDataType.TEXT;
      default:
        throw new NotImplementedException("Type: " + type.getFullTypeString() + " no yet implemented!");
    }
  }

  public DataContext getContext(QueryContext queryContext) {
    DataContext inner = DataContexts.of(Collections.emptyMap());

    return new DataContext() {

      @Override
      public @Nullable SchemaPlus getRootSchema() {
        return rootSchema.plus();
      }

      @Override
      public JavaTypeFactory getTypeFactory() {
        return typeFactory;
      }

      @Override
      public QueryProvider getQueryProvider() {
        return inner.getQueryProvider();
      }

      @Override
      public @Nullable Object get(String name) {
        if ("series".equals(name)) {
          // Return an Enumerable here
          Function2<String, TSDataType, Enumerable> producer = new Function2<String, TSDataType, Enumerable>() {
            @Override
            public Enumerable apply(String s, TSDataType dataType) {
              SeriesIterator series;
              try {
                PartialPath path = new PartialPath(s);
                series = new SeriesIterator(null, BatchReaderUtil.getReaderInQuery(queryContext, path, dataType, null, true), null);
              } catch (IllegalPathException e) {
                throw new IllegalStateException();
              }
              return Linq4j.asEnumerable(new Iterable<Object[]>() {
                @Override
                public Iterator<Object[]> iterator() {
                  return series;
                }
              });
            }
          };
          return producer;
        }
        return inner.get(name);
      }
    };
  }

  public static abstract class TSJoin extends BiRel {

    protected TSJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right) {
      super(cluster, traitSet, left, right);
    }

  }

  public static final class LogicalTSJoin extends TSJoin {

    public LogicalTSJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right) {
      super(cluster, traitSet, left, right);
    }
  }

  public RelNode toRelNode(RawDataQueryPlan queryPlan) {
    if (!(queryPlan.getPaths().size() == 1)) {
      throw new UnsupportedOperationException();
    }
    relBuilder.scan(queryPlan.getPaths().get(0).getFullPath());
    if (queryPlan.getExpression() != null) {
      transform(queryPlan.getExpression());
    }
    return relBuilder.build();
  }

  public void transform(IExpression expression) {
    if (expression.getType() == ExpressionType.SERIES) {
      // Add a Filter stage
      SingleSeriesExpression seriesExpression = (SingleSeriesExpression) expression;

      relBuilder.scan(seriesExpression.getSeriesPath().getFullPath());
//      if (seriesExpression.getFilter())
//      relBuilder.filter()
      relBuilder.join(JoinRelType.LEFT, "time");
//      relBuilder.project(
//          relBuilder.field(0),
//          relBuilder.field(1),
//          relBuilder.field(3)
//      );
      relBuilder.sort(0);
      relBuilder.filter(relBuilder.greaterThan(relBuilder.field(3), rexBuilder.makeLiteral(100, typeFactory.createJavaType(Long.class))));
      relBuilder.project(relBuilder.field(0), relBuilder.field(1));
      relBuilder.sort(0);
      return;
    }
    throw new NotImplementedException();
  }

}
