package org.apache.iotdb.confignode.consensus.request.read.ttl;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ShowTTLPlan extends ConfigPhysicalPlan {
  public ShowTTLPlan() {
    super(ConfigPhysicalPlanType.ShowTTL);
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {}

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {}
}
