package org.apache.iotdb.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import sun.security.util.ObjectIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Rules and relational operators for
 * {@link IoTDBRel#CONVENTION}
 * calling convention.
 */
public class IoTDBRules {
  private IoTDBRules() {}

  public static final RelOptRule[] RULES = {
      IoTDBFilterRule.INSTANCE,
      IoTDBProjectRule.INSTANCE,
      IoTDBLimitRule.INSTANCE
  };

  static List<String> IoTDBFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
            SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /** Translator from {@link RexNode} to strings in IoTDB's expression
   * language. */
  static class RexToIoTDBTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    protected RexToIoTDBTranslator(JavaTypeFactory typeFactory,
                                       List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

   /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link IoTDBFilter}.
   */
  private static class IoTDBFilterRule extends ConverterRule {
    private static final IoTDBFilterRule INSTANCE = new IoTDBFilterRule();

    private IoTDBFilterRule() {
      super(LogicalFilter.class, Convention.NONE, IoTDBRel.CONVENTION, "IoTDBFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(IoTDBRel.CONVENTION);
      try {
        return new IoTDBFilter(filter.getCluster(), traitSet,
                convert(filter.getInput(), IoTDBRel.CONVENTION), filter.getCondition());
      } catch (LogicalOptimizeException e) {
        throw new AssertionError(e.getMessage());
      }
    }
  }

   /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link IoTDBFilter}.
   */
/*  private static class IoTDBFilterRule extends RelOptRule {
    private static final IoTDBFilterRule INSTANCE = new IoTDBFilterRule();

    private IoTDBFilterRule() {
      super(operand(LogicalFilter.class, operand(IoTDBTableScan.class, none())),
              "IoTDBFilterRule");
    }

    public RelNode convert(LogicalFilter filter) {
      final RelTraitSet traitSet = filter.getTraitSet().replace(IoTDBRel.CONVENTION);
      try {
        return new IoTDBFilter(filter.getCluster(), traitSet,
                convert(filter.getInput(), IoTDBRel.CONVENTION), filter.getCondition());
      } catch (LogicalOptimizeException e) {
        throw new AssertionError(e.getMessage());
      }
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }
  }*/

  /**
   * Rule to convert a {@link LogicalProject} to a {@link IoTDBProject}.
   */
  private static class IoTDBProjectRule extends ConverterRule {
    private static final IoTDBProjectRule INSTANCE = new IoTDBProjectRule();
    private IoTDBProjectRule() {
      super(LogicalProject.class, Convention.NONE, IoTDBRel.CONVENTION , "IoTDBProjectRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      for (RexNode e : project.getProjects()) {
        if (!(e instanceof RexInputRef)) {
          return false;
        }
      }
      return true;
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(IoTDBRel.CONVENTION);
      return new IoTDBProject(project.getCluster(), traitSet,
              convert(project.getInput(), IoTDBRel.CONVENTION), project.getProjects(),
              project.getRowType());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.adapter.enumerable.EnumerableLimit} to a
   * {@link IoTDBLimit}.
   */
  private static class IoTDBLimitRule extends RelOptRule {
    private static final IoTDBLimitRule INSTANCE = new IoTDBLimitRule();

    private IoTDBLimitRule() {
      super(operand(EnumerableLimit.class, operand(IoTDBToEnumerableConverter.class, any())),
              "IoTDBLimitRule");
    }

    public RelNode convert(EnumerableLimit limit) {
      final RelTraitSet traitSet = limit.getTraitSet().replace(IoTDBRel.CONVENTION);
      return new IoTDBLimit(limit.getCluster(), traitSet,
              convert(limit.getInput(), IoTDBRel.CONVENTION), limit.offset, limit.fetch);
    }

    /** @see org.apache.calcite.rel.convert.ConverterRule */
    public void onMatch(RelOptRuleCall call) {
      final EnumerableLimit limit = call.rel(0);
      final RelNode converted = convert(limit);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }
}
