package org.apache.iotdb.confignode.consensus.request.write.template;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DropSchemaTemplatePlan extends ConfigPhysicalPlan {

  private String templateName;

  public DropSchemaTemplatePlan() {
    super(ConfigPhysicalPlanType.DropSchemaTemplate);
  }

  public DropSchemaTemplatePlan(String templateName) {
    super(ConfigPhysicalPlanType.DropSchemaTemplate);
    this.templateName = templateName;
  }

  public String getTemplateName() {
    return templateName;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigPhysicalPlanType.DropSchemaTemplate.ordinal());
    ReadWriteIOUtils.write(templateName, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.templateName = ReadWriteIOUtils.readString(buffer);
  }
}
