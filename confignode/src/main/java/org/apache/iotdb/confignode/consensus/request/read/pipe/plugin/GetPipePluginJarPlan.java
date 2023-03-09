package org.apache.iotdb.confignode.consensus.request.read.pipe.plugin;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetPipePluginJarPlan extends ConfigPhysicalPlan {
  private List<String> jarNames;

  public GetPipePluginJarPlan() {
    super(ConfigPhysicalPlanType.GetPipePluginJar);
  }

  public GetPipePluginJarPlan(List<String> jarNames) {
    super(ConfigPhysicalPlanType.GetPipePluginJar);
    this.jarNames = jarNames;
  }

  public List<String> getJarNames() {
    return jarNames;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    ReadWriteIOUtils.write(jarNames.size(), stream);
    for (String jarName : jarNames) {
      ReadWriteIOUtils.write(jarName, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = ReadWriteIOUtils.readInt(buffer);
    jarNames = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      jarNames.add(ReadWriteIOUtils.readString(buffer));
    }
  }
}
