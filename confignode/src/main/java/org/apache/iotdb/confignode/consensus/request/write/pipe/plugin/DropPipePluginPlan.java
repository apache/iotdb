package org.apache.iotdb.confignode.consensus.request.write.pipe.plugin;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DropPipePluginPlan extends ConfigPhysicalPlan {
  private String pluginName;

  public DropPipePluginPlan() {
    super(ConfigPhysicalPlanType.DropPipePlugin);
  }

  public DropPipePluginPlan(String pluginName) {
    super(ConfigPhysicalPlanType.DropPipePlugin);
    this.pluginName = pluginName;
  }

  public String getPluginName() {
    return pluginName;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(pluginName, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pluginName = ReadWriteIOUtils.readString(buffer);
  }
}
