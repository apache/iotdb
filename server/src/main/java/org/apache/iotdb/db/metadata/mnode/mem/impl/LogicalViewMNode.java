package org.apache.iotdb.db.metadata.mnode.mem.impl;

import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.info.IMeasurementInfo;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.mem.IMemMNode;
import org.apache.iotdb.db.metadata.mnode.mem.basic.BasicMNode;
import org.apache.iotdb.db.metadata.mnode.mem.container.MemMNodeContainer;
import org.apache.iotdb.db.metadata.mnode.mem.info.LogicalViewInfo;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;

public class LogicalViewMNode extends AbstractMeasurementMNode<IMemMNode, BasicMNode>
    implements IMemMNode {

  public LogicalViewMNode(
      IDeviceMNode<IMemMNode> parent, String name, ViewExpression viewExpression) {
    super(
        new BasicMNode(parent == null ? null : parent.getAsMNode(), name),
        new LogicalViewInfo(new LogicalViewSchema(name, viewExpression)));
  }

  @Override
  public IMNodeContainer<IMemMNode> getChildren() {
    return MemMNodeContainer.emptyMNodeContainer();
  }

  @Override
  public IMemMNode getAsMNode() {
    return this;
  }

  public void setExpression(ViewExpression expression) {
    IMeasurementInfo measurementInfo = this.getMeasurementInfo();
    if (measurementInfo instanceof LogicalViewInfo) {
      ((LogicalViewInfo) measurementInfo).setExpression(expression);
    }
  }

  @Override
  public final boolean isLogicalView() {
    return true;
  }
}
