package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeleteTimeSeriesNode extends WritePlanNode {

  private final List<PartialPath> pathList;

  public DeleteTimeSeriesNode(PlanNodeId id, List<PartialPath> pathList) {
    super(id);
    this.pathList = pathList;
  }

  public List<PartialPath> getPathList() {
    return pathList;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new DeleteTimeSeriesNode(getPlanNodeId(), pathList);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DELETE_TIMESERIES.serialize(byteBuffer);
    ReadWriteIOUtils.write(pathList.size(), byteBuffer);
    for (PartialPath path : pathList) {
      path.serialize(byteBuffer);
    }
  }

  public static DeleteTimeSeriesNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<PartialPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      pathList.add((PartialPath) PathDeserializeUtil.deserialize(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeleteTimeSeriesNode(planNodeId, pathList);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    return null;
  }
}
