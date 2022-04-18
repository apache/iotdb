package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SourceNode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class StorageGroupSchemaScanNode extends SourceNode {

  private final List<String> storageGroups;

  public StorageGroupSchemaScanNode(PlanNodeId planNodeId, @NotNull List<String> storageGroups) {
    super(planNodeId);
    this.storageGroups = storageGroups;
  }

  public static PlanNode deserialize(ByteBuffer byteBuffer) {
    List<String> storageGroups = ReadWriteIOUtils.readStringList(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new StorageGroupSchemaScanNode(planNodeId, storageGroups);
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new StorageGroupSchemaScanNode(getPlanNodeId(), storageGroups);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.STORAGE_GROUP_SCHEMA_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.writeStringList(storageGroups, byteBuffer);
  }

  @Override
  public void open() {}

  @Override
  public RegionReplicaSet getRegionReplicaSet() {
    //todo return a special replicaset witch has only one endpoint: local ip
    return null;
  }

  @Override
  public void setRegionReplicaSet(RegionReplicaSet regionReplicaSet) {}

  @Override
  public void close() throws Exception {}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    StorageGroupSchemaScanNode that = (StorageGroupSchemaScanNode) o;
    return storageGroups.equals(that.storageGroups);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), storageGroups);
  }
}
