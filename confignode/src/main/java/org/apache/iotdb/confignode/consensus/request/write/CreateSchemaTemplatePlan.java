package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author chenhuangyun
 * @date 2022/7/5
 */
public class CreateSchemaTemplatePlan extends ConfigPhysicalPlan {

  private byte[] template;

  public CreateSchemaTemplatePlan() {
    super(ConfigPhysicalPlanType.CreateSchemaTemplate);
  }

  public CreateSchemaTemplatePlan(byte[] template) {
    this();
    this.template = template;
  }

  public byte[] getTemplate() {
    return template;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(ConfigPhysicalPlanType.CreateSchemaTemplate.ordinal());
    stream.writeInt(template.length);
    stream.write(template);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    /*try {
        template = Template.byteBuffer2Template(buffer);
    }catch (Exception e) {
        e.printStackTrace();
    }*/
    int length = ReadWriteIOUtils.readInt(buffer);
    this.template = ReadWriteIOUtils.readBytes(buffer, length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateSchemaTemplatePlan that = (CreateSchemaTemplatePlan) o;
    return template.equals(that.template);
  }

  @Override
  public int hashCode() {
    return Objects.hash(template);
  }
}
