package org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class InvalidateSchemaCacheNode extends WritePlanNode {

  private final QueryId queryId;

  private final List<PartialPath> pathList;

  private final List<String> storageGroups;

  public InvalidateSchemaCacheNode(
      PlanNodeId id, QueryId queryId, List<PartialPath> pathList, List<String> storageGroups) {
    super(id);
    this.queryId = queryId;
    this.pathList = pathList;
    this.storageGroups = storageGroups;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public List<PartialPath> getPathList() {
    return pathList;
  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new InvalidateSchemaCacheNode(getPlanNodeId(), queryId, pathList, storageGroups);
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
    PlanNodeType.INVALIDATE_SCHEMA_CACHE.serialize(byteBuffer);
    queryId.serialize(byteBuffer);
    ReadWriteIOUtils.write(pathList.size(), byteBuffer);
    for (PartialPath path : pathList) {
      path.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(storageGroups.size(), byteBuffer);
    for (String storageGroup : storageGroups) {
      ReadWriteIOUtils.write(storageGroup, byteBuffer);
    }
  }

  public static InvalidateSchemaCacheNode deserialize(ByteBuffer byteBuffer) {
    QueryId queryId = QueryId.deserialize(byteBuffer);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<PartialPath> pathList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      pathList.add((PartialPath) PathDeserializeUtil.deserialize(byteBuffer));
    }
    size = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> storageGroups = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      storageGroups.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new InvalidateSchemaCacheNode(planNodeId, queryId, pathList, storageGroups);
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
