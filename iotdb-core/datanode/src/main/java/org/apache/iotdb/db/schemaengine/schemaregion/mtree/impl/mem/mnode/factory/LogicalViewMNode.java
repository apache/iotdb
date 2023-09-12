package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.factory;

import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.basic.BasicMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.container.MemMNodeContainer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.LogicalViewInfo;

public class LogicalViewMNode extends AbstractMeasurementMNode<IMemMNode, BasicMNode>
    implements IMemMNode {

  public LogicalViewMNode(
      IDeviceMNode<IMemMNode> parent, String name, LogicalViewSchema measurementSchema) {
    super(
        new BasicMNode(parent == null ? null : parent.getAsMNode(), name),
        new LogicalViewInfo(measurementSchema));
  }

  @Override
  public IMNodeContainer<IMemMNode> getChildren() {
    return MemMNodeContainer.emptyMNodeContainer();
  }

  @Override
  public IMemMNode getAsMNode() {
    return this;
  }

  @Override
  public final boolean isLogicalView() {
    return true;
  }
}
