package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author zhanghang
 * @date 2021/10/22 16:34
 */
public class UnsetSchemaTemplatePlan extends PhysicalPlan {

  String prefixPath;
  String templateName;

  public UnsetSchemaTemplatePlan() {
    super(false, Operator.OperatorType.UNSET_SCHEMA_TEMPLATE);
  }

  public UnsetSchemaTemplatePlan(String prefixPath, String templateName) {
    super(false, Operator.OperatorType.UNSET_SCHEMA_TEMPLATE);
    this.prefixPath = prefixPath;
    this.templateName = templateName;
  }

  public String getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(String prefixPath) {
    this.prefixPath = prefixPath;
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.UNSET_SCHEMA_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(prefixPath, buffer);
    ReadWriteIOUtils.write(templateName, buffer);

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    prefixPath = ReadWriteIOUtils.readString(buffer);
    templateName = ReadWriteIOUtils.readString(buffer);

    this.index = buffer.getLong();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.UNSET_SCHEMA_TEMPLATE.ordinal());

    ReadWriteIOUtils.write(prefixPath, stream);
    ReadWriteIOUtils.write(templateName, stream);

    stream.writeLong(index);
  }
}
