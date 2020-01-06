package org.apache.iotdb.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a relational expression from
 * {@link IoTDBRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class IoTDBToEnumerableConverterRule extends ConverterRule {
  public static final ConverterRule INSTANCE =
          new IoTDBToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

  /**
   * Creates a IoTDBToEnumerableConverterRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public IoTDBToEnumerableConverterRule(
          RelBuilderFactory relBuilderFactory) {
    super(RelNode.class, (Predicate<RelNode>) r -> true,
            IoTDBRel.CONVENTION, EnumerableConvention.INSTANCE,
            relBuilderFactory, "IoTDBToEnumerableConverterRule");
  }

  @Override public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new IoTDBToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}

// End IoTDBToEnumerableConverterRule.java