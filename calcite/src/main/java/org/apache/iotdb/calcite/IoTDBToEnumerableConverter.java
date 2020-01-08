package org.apache.iotdb.calcite;

import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Relational expression representing a scan of a table in a IoTDB data source.
 */
public class IoTDBToEnumerableConverter extends ConverterImpl
        implements EnumerableRel {
  public IoTDBToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }
  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IoTDBToEnumerableConverter(
            getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                              RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }
  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generates a call to "query" with the appropriate fields and predicates
    final BlockBuilder list = new BlockBuilder();
    final IoTDBRel.Implementor IoTDBImplementor = new IoTDBRel.Implementor();
    IoTDBImplementor.visitChild(0, getInput());
    final RelDataType rowType = getRowType();
    final PhysType physType =
            PhysTypeImpl.of(
                    implementor.getTypeFactory(), rowType,
                    pref.prefer(JavaRowFormat.ARRAY));
    final Expression fields =
            list.append("fields",
                    constantArrayList(
                            Pair.zip(IoTDBRules.IoTDBFieldNames(rowType),
                                    new AbstractList<Class>() {
                                      @Override public Class get(int index) {
                                        return physType.fieldClass(index);
                                      }

                                      @Override public int size() {
                                        return rowType.getFieldCount();
                                      }
                                    }),
                            Pair.class));

    final Expression selectFields =
            list.append("selectFields",
                    constantArrayList(IoTDBImplementor.selectFields, String.class));
    final Expression table =
            list.append("table",
                    IoTDBImplementor.table.getExpression(
                            IoTDBTable.IoTDBQueryable.class));
    final Expression devices =
            list.append("devices",
                    constantArrayList(IoTDBImplementor.fromClause, String.class));
    final Expression predicates =
            list.append("predicates",
                    constantArrayList(IoTDBImplementor.whereClause, String.class));
    final Expression limit =
            list.append("limit",
                    Expressions.constant(IoTDBImplementor.limit));
    final Expression offset =
            list.append("offset",
                    Expressions.constant(IoTDBImplementor.offset));
    Expression enumerable =
            list.append("enumerable",
                    Expressions.call(table,
                            IoTDBMethod.IoTDB_QUERYABLE_QUERY.method, fields,
                            selectFields, devices, predicates, limit, offset));
    if (CalciteSystemProperty.DEBUG.value()) {
      System.out.println("IoTDB: " + predicates);
    }
    Hook.QUERY_PLAN.run(predicates);
    list.add(
            Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  /** E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')". */
  private static <T> MethodCallExpression constantArrayList(List<T> values,
                                                            Class clazz) {
    return Expressions.call(
            BuiltInMethod.ARRAYS_AS_LIST.method,
            Expressions.newArrayInit(clazz, constantList(values)));
  }

  /** E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}. */
  private static <T> List<Expression> constantList(List<T> values) {
    return Lists.transform(values, Expressions::constant);
  }
}
