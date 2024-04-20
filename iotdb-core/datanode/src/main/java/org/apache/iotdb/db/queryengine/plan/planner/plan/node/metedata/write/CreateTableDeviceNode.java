package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CreateTableDeviceNode extends WritePlanNode {

  private final List<PartialPath> devicePathList;

  private final List<String> attributeNameList;

  private final List<List<String>> attributeValueList;

  private TRegionReplicaSet regionReplicaSet;

  public CreateTableDeviceNode(
      PlanNodeId id,
      List<PartialPath> devicePathList,
      List<String> attributeNameList,
      List<List<String>> attributeValueList) {
    super(id);
    this.devicePathList = devicePathList;
    this.attributeNameList = attributeNameList;
    this.attributeValueList = attributeValueList;
  }

  public CreateTableDeviceNode(
      PlanNodeId id,
      TRegionReplicaSet regionReplicaSet,
      List<PartialPath> devicePathList,
      List<String> attributeNameList,
      List<List<String>> attributeValueList) {
    super(id);
    this.devicePathList = devicePathList;
    this.attributeNameList = attributeNameList;
    this.attributeValueList = attributeValueList;
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.CREATE_TABLE_DEVICE;
  }

  public List<PartialPath> getDevicePathList() {
    return devicePathList;
  }

  public List<String> getAttributeNameList() {
    return attributeNameList;
  }

  public List<List<String>> getAttributeValueList() {
    return attributeValueList;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return new ArrayList<>();
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return new CreateTableDeviceNode(
        getPlanNodeId(), regionReplicaSet, devicePathList, attributeNameList, attributeValueList);
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
    PlanNodeType.CREATE_TABLE_DEVICE.serialize(byteBuffer);
    ReadWriteIOUtils.write(devicePathList.size(), byteBuffer);
    for (PartialPath deviceId : devicePathList) {
      deviceId.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(attributeNameList.size(), byteBuffer);
    for (String attributeName : attributeNameList) {
      ReadWriteIOUtils.write(attributeName, byteBuffer);
    }
    ReadWriteIOUtils.write(attributeValueList.size(), byteBuffer);
    for (List<String> deviceValueList : attributeValueList) {
      ReadWriteIOUtils.write(deviceValueList.size(), byteBuffer);
      for (String value : deviceValueList) {
        ReadWriteIOUtils.write(value, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.CREATE_TABLE_DEVICE.serialize(stream);
    ReadWriteIOUtils.write(devicePathList.size(), stream);
    for (PartialPath deviceId : devicePathList) {
      deviceId.serialize(stream);
    }
    ReadWriteIOUtils.write(attributeNameList.size(), stream);
    for (String attributeName : attributeNameList) {
      ReadWriteIOUtils.write(attributeName, stream);
    }
    for (List<String> deviceValueList : attributeValueList) {
      for (String value : deviceValueList) {
        ReadWriteIOUtils.write(value, stream);
      }
    }
  }

  public static CreateTableDeviceNode deserialize(ByteBuffer buffer) {
    int deviceNum = ReadWriteIOUtils.readInt(buffer);
    List<PartialPath> devicePathList = new ArrayList<>(deviceNum);
    for (int i = 0; i < deviceNum; i++) {
      devicePathList.add((PartialPath) PathDeserializeUtil.deserialize(buffer));
    }
    int attributeNameNum = ReadWriteIOUtils.readInt(buffer);
    List<String> attributeNameList = new ArrayList<>(attributeNameNum);
    for (int i = 0; i < attributeNameNum; i++) {
      attributeNameList.add(ReadWriteIOUtils.readString(buffer));
    }
    List<List<String>> attributeValueList = new ArrayList<>(deviceNum);
    for (int i = 0; i < deviceNum; i++) {
      List<String> deviceValueList = new ArrayList<>(attributeNameNum);
      for (int j = 0; j < attributeNameNum; j++) {
        deviceValueList.add(ReadWriteIOUtils.readString(buffer));
      }
      attributeValueList.add(deviceValueList);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new CreateTableDeviceNode(
        planNodeId, devicePathList, attributeNameList, attributeValueList);
  }

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    for (int i = 0; i < devicePathList.size(); i++) {
      TRegionReplicaSet regionReplicaSet =
          analysis
              .getSchemaPartitionInfo()
              .getSchemaRegionReplicaSet(devicePathList.get(i).getFullPath());
      splitMap.computeIfAbsent(regionReplicaSet, k -> new ArrayList<>()).add(i);
    }
    List<WritePlanNode> result = new ArrayList<>(splitMap.size());
    for (Map.Entry<TRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      List<PartialPath> subDevicePathList = new ArrayList<>(entry.getValue().size());
      List<List<String>> subAttributeValueList = new ArrayList<>(entry.getValue().size());
      for (Integer index : entry.getValue()) {
        subDevicePathList.add(devicePathList.get(index));
        subAttributeValueList.add(attributeValueList.get(index));
      }
      result.add(
          new CreateTableDeviceNode(
              getPlanNodeId(),
              entry.getKey(),
              subDevicePathList,
              attributeNameList,
              subAttributeValueList));
    }
    return result;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTableDevice(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CreateTableDeviceNode)) return false;
    if (!super.equals(o)) return false;
    CreateTableDeviceNode that = (CreateTableDeviceNode) o;
    return Objects.equals(devicePathList, that.devicePathList)
        && Objects.equals(attributeNameList, that.attributeNameList)
        && Objects.equals(attributeValueList, that.attributeValueList)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), devicePathList, attributeNameList, attributeValueList, regionReplicaSet);
  }
}
