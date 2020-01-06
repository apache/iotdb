package org.apache.iotdb.calcite;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
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
      IoTDBProjectRule.INSTANCE
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

  /** Base class for planner rules that convert a relational expression to
   * IoTDB calling convention. */
  abstract static class IoTDBConverterRule extends ConverterRule {
    protected final Convention out;

    IoTDBConverterRule(Class<? extends RelNode> clazz,
                           String description) {
      this(clazz, r -> true, description);
    }

    <R extends RelNode> IoTDBConverterRule(Class<R> clazz,
                                           Predicate<? super R> predicate,
                                           String description) {
      super(clazz, predicate, Convention.NONE,
              IoTDBRel.CONVENTION, RelFactories.LOGICAL_BUILDER, description);
      this.out = IoTDBRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link LogicalFilter} to a
   * {@link IoTDBFilter}.
   */
  private static class IoTDBFilterRule extends IoTDBConverterRule {
    private static final IoTDBFilterRule INSTANCE = new IoTDBFilterRule();

    private IoTDBFilterRule() {
      super(LogicalFilter.class, "IoTDBFilterRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      RexCall condition = (RexCall) filter.getCondition();

      return containsDeviceId(condition);
    }

    // check if condition contains deviceid, if true then return false
    private boolean containsDeviceId(RexCall condition){
      if(condition == null)
        return true;
      // 默认变量在左，value 在右
      if(condition.getOperands().get(0) instanceof RexInputRef){
        RexInputRef ref = (RexInputRef) condition.getOperands().get(0);
        if(ref.getIndex() == 1){
          return false;
        }
        return true;
      }
      else{
        RexCall left = (RexCall) condition.getOperands().get(0);
        RexCall right = null;
        if(condition.getOperands().size() == 2){
          right = (RexCall) condition.getOperands().get(1);
        }
        return containsDeviceId(left) && containsDeviceId(right);
      }
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
/*      return new IoTDBFilter(filter.getCluster(), traitSet,
              convert(filter.getInput(), out), filter.getCondition());*/
      return new IoTDBFilter(filter.getCluster(), traitSet,
              filter.getInput(), filter.getCondition());
    }

  }

  /**
   * Rule to convert a {@link LogicalProject}
   * to a {@link IoTDBProject}.
   */
  private static class IoTDBProjectRule extends IoTDBConverterRule {
    private static final IoTDBProjectRule INSTANCE = new IoTDBProjectRule();
    private IoTDBProjectRule() {
      super(LogicalProject.class, "IoTDBProjectRule");
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
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new IoTDBProject(project.getCluster(), traitSet,
              convert(project.getInput(), out), project.getProjects(),
              project.getRowType());
    }

  }
}
