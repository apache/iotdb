package org.apache.iotdb.db.qp.physical.crud;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class SetDeviceTemplatePlan extends PhysicalPlan {
  String templateName;
  String prefixPath;

  public SetDeviceTemplatePlan() {
    super(false, OperatorType.SET_DEVICE_TEMPLATE);
  }

  public SetDeviceTemplatePlan(String templateName, String prefixPath) {
    super(false, OperatorType.SET_DEVICE_TEMPLATE);
    this.templateName = templateName;
    this.prefixPath = prefixPath;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.SET_DEVICE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(templateName, buffer);
    ReadWriteIOUtils.write(prefixPath, buffer);

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    templateName = ReadWriteIOUtils.readString(buffer);
    prefixPath = ReadWriteIOUtils.readString(buffer);

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.SET_DEVICE_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(prefixPath, stream);

    stream.writeLong(index);
  }
}
