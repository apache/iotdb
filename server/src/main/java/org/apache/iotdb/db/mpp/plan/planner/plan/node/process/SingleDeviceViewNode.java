package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SingleDeviceViewNode extends SingleChildProcessNode {

  private String device;
  private final List<String> outputColumnNames;
  private final List<Integer> deviceToMeasurementIndexes;

  public SingleDeviceViewNode(
      PlanNodeId id,
      List<String> outputColumnNames,
      String device,
      List<Integer> deviceToMeasurementIndexes) {
    super(id);
    this.device = device;
    this.outputColumnNames = outputColumnNames;
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexes;
  }

  public SingleDeviceViewNode(
      PlanNodeId id, List<String> outputColumnNames, List<Integer> deviceToMeasurementIndexesMap) {
    super(id);
    this.outputColumnNames = outputColumnNames;
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexesMap;
  }

  public void setChildDeviceNode(String deviceName, PlanNode childNode) {
    this.device = deviceName;
    setChild(childNode);
  }

  @Override
  public PlanNode clone() {
    return new SingleDeviceViewNode(
        getPlanNodeId(), outputColumnNames, device, deviceToMeasurementIndexes);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public String getDevice() {
    return device;
  }

  public List<Integer> getDeviceToMeasurementIndexes() {
    return deviceToMeasurementIndexes;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSingleDeviceView(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SINGLE_DEVICE_VIEW.serialize(byteBuffer);
    ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
    for (String column : outputColumnNames) {
      ReadWriteIOUtils.write(column, byteBuffer);
    }
    ReadWriteIOUtils.write(device, byteBuffer);
    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), byteBuffer);
    for (Integer index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_VIEW.serialize(stream);
    ReadWriteIOUtils.write(outputColumnNames.size(), stream);
    for (String column : outputColumnNames) {
      ReadWriteIOUtils.write(column, stream);
    }
    ReadWriteIOUtils.write(device, stream);
    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), stream);
    for (Integer index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, stream);
    }
  }

  public static SingleDeviceViewNode deserialize(ByteBuffer byteBuffer) {
    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumnNames = new ArrayList<>();
    while (columnSize > 0) {
      outputColumnNames.add(ReadWriteIOUtils.readString(byteBuffer));
      columnSize--;
    }
    String device = ReadWriteIOUtils.readString(byteBuffer);
    int listSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> deviceToMeasurementIndexes = new ArrayList<>(listSize);
    while (listSize > 0) {
      deviceToMeasurementIndexes.add(ReadWriteIOUtils.readInt(byteBuffer));
      listSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SingleDeviceViewNode(
        planNodeId, outputColumnNames, device, deviceToMeasurementIndexes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SingleDeviceViewNode that = (SingleDeviceViewNode) o;
    return device.equals(that.device)
        && outputColumnNames.equals(that.outputColumnNames)
        && deviceToMeasurementIndexes.equals(that.deviceToMeasurementIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), device, outputColumnNames, deviceToMeasurementIndexes);
  }

  @Override
  public String toString() {
    return "SingleDeviceView-" + this.getPlanNodeId();
  }
}
