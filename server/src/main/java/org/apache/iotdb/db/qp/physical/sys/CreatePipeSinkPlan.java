package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CreatePipeSinkPlan extends PhysicalPlan {
  private String pipeSinkName;
  private String pipeSinkType;
  private List<Pair<String, String>> pipeSinkAttributes;

  public CreatePipeSinkPlan(String pipeSinkName, String pipeSinkType) {
    super(Operator.OperatorType.CREATE_PIPESINK);
    this.pipeSinkName = pipeSinkName;
    this.pipeSinkType = pipeSinkType;
    pipeSinkAttributes = new ArrayList<>();
  }

  public void addPipeSinkAttribute(String attr, String value) {
    pipeSinkAttributes.add(new Pair<>(attr, value));
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }

  public String getPipeSinkType() {
    return pipeSinkType;
  }

  public List<Pair<String, String>> getPipeSinkAttributes() {
    return pipeSinkAttributes;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
