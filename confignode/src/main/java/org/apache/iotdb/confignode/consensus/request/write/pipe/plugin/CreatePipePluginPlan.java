package org.apache.iotdb.confignode.consensus.request.write.pipe.plugin;

import org.apache.iotdb.commons.pipe.plugin.PipePluginInformation;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CreatePipePluginPlan extends ConfigPhysicalPlan {
  private PipePluginInformation pipePluginInformation;

  private Binary jarFile;

  public CreatePipePluginPlan() {
    super(ConfigPhysicalPlanType.CreatePipePlugin);
  }

  public CreatePipePluginPlan(PipePluginInformation pipePluginInformation, Binary jarFile) {
    super(ConfigPhysicalPlanType.CreatePipePlugin);
    this.pipePluginInformation = pipePluginInformation;
    this.jarFile = jarFile;
  }

  public PipePluginInformation getPipePluginInformation() {
    return pipePluginInformation;
  }

  public Binary getJarFile() {
    return jarFile;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    pipePluginInformation.serialize(stream);
    if (jarFile == null) {
      ReadWriteIOUtils.write(true, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
      ReadWriteIOUtils.write(jarFile, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipePluginInformation = PipePluginInformation.deserialize(buffer);
    if (ReadWriteIOUtils.readBool(buffer)) {
      return;
    }
    jarFile = ReadWriteIOUtils.readBinary(buffer);
  }
}
