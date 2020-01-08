package org.apache.iotdb.calcite;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Project}
 * relational expression in IoTDB.
 */
public class IoTDBProject extends Project implements IoTDBRel {

  public IoTDBProject(RelOptCluster cluster, RelTraitSet traitSet,
                      RelNode input, List<? extends RexNode> projects, RelDataType rowType){
    super(cluster, traitSet, input, projects, rowType);
    assert getConvention() == IoTDBRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public Project copy(RelTraitSet traitSet, RelNode input,
                                List<RexNode> projects, RelDataType rowType) {
    return new IoTDBProject(getCluster(), traitSet, input, projects,
            rowType);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                              RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    final IoTDBRules.RexToIoTDBTranslator translator =
            new IoTDBRules.RexToIoTDBTranslator(
                    (JavaTypeFactory) getCluster().getTypeFactory(),
                    IoTDBRules.IoTDBFieldNames(getInput().getRowType()));
    final List<String> selectFields = new ArrayList<>();
    for (Pair<RexNode, String> pair : getNamedProjects()) {
      final String originalName = pair.left.accept(translator);
      selectFields.add(originalName);
    }
    implementor.addFields(selectFields);
  }
}

// End IoTDBProject.java
