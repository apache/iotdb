package org.apache.iotdb.confignode.consensus.request.write.confignode;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateConfigNodePlan extends ConfigPhysicalPlan {
  private TConfigNodeLocation configNodeLocation;

  public UpdateConfigNodePlan() {
    super(ConfigPhysicalPlanType.UpdateConfigNodeLocation);
  }

  public UpdateConfigNodePlan(TConfigNodeLocation configNodeLocation) {
    this();
    this.configNodeLocation = configNodeLocation;
  }

  public TConfigNodeLocation getConfigNodeLocation() {
    return configNodeLocation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(configNodeLocation, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    configNodeLocation = ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!getType().equals(((UpdateConfigNodePlan) o).getType())) {
      return false;
    }
    UpdateConfigNodePlan that = (UpdateConfigNodePlan) o;
    return configNodeLocation.equals(that.configNodeLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), configNodeLocation);
  }
}
