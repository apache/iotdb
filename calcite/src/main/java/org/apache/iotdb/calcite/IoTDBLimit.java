package org.apache.iotdb.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Implementation of limits in IoTDB.
 */
public class IoTDBLimit extends SingleRel implements IoTDBRel {
  public final RexNode limit;
  public final RexNode offset;

  public IoTDBLimit(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input);
    this.limit = fetch;
    this.offset = offset;
    assert getConvention() == input.getConvention();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                              RelMetadataQuery mq) {
    // We do this so we get the limit for free
    return planner.getCostFactory().makeZeroCost();
  }

  @Override public IoTDBLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new IoTDBLimit(getCluster(), traitSet, sole(newInputs), offset, limit);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if (limit != null) {
      implementor.limit = RexLiteral.intValue(limit);
    }
    if (offset != null) {
      implementor.offset = RexLiteral.intValue(offset);
    }
  }

  // explain this Limit RelNode in physical plan
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.itemIf("limit", limit, limit != null);
    pw.itemIf("offset", offset, offset != null);
    return pw;
  }
}

// End IoTDBLimit.java