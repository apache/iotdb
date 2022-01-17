package org.apache.iotdb.db.query.executor.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iotdb.db.query.executor.IoTDBRules;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class IoTDBTableScan extends TableScan implements IoTDBRel {
  final IoTDBTable iotdbTable;
  final RelDataType projectRowType;

  /**
   * Creates an IoTDBTableScan.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param iotdbTable     MongoDB table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  protected IoTDBTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                           RelOptTable table, IoTDBTable iotdbTable, RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.iotdbTable = iotdbTable;
    this.projectRowType = projectRowType;

    assert iotdbTable != null;
    assert getConvention() == IoTDBRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                        RelMetadataQuery mq) {
    // scans with a small project list are cheaper
    final float f = projectRowType == null ? 1f
        : (float) projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(IoTDBToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : IoTDBRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(Implementor implementor) {
    implementor.ioTDBTable = this.iotdbTable;
    implementor.table = this.table;
  }
}
