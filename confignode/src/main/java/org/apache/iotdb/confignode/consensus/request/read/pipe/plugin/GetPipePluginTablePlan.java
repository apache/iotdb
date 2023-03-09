package org.apache.iotdb.confignode.consensus.request.read.pipe.plugin;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class GetPipePluginTablePlan extends ConfigPhysicalPlan {

  public GetPipePluginTablePlan() {
    super(ConfigPhysicalPlanType.GetPipePluginTable);
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {}
}
