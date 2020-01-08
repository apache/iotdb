package org.apache.iotdb.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Relational expression representing a scan of a IoTDB storage group.
 */
public class IoTDBTableScan extends TableScan implements IoTDBRel {
  final IoTDBTable ioTDBTable;
  final RelDataType projectRowType;

  /**
   * Creates a IoTDBTableScan.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param ioTDBTable IoTDB table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  protected IoTDBTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                           RelOptTable table, IoTDBTable ioTDBTable, RelDataType projectRowType) {
    super(cluster, traitSet, table);
    this.ioTDBTable = ioTDBTable;
    this.projectRowType = projectRowType;

    assert ioTDBTable != null;
    assert getConvention() == IoTDBRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // scans with a small project list are cheaper
    final float f = projectRowType == null ? 1f : (float) projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(IoTDBToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : IoTDBRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.ioTDBTable = ioTDBTable;
    implementor.table = table;
  }
}

// End IoTDBTableScan.java