package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class RemoveConfigNodeReq extends ConfigRequest {

  private TConfigNodeLocation configNodeLocation;

  public RemoveConfigNodeReq() {
    super(ConfigRequestType.RemoveConfigNode);
  }

  public RemoveConfigNodeReq(TConfigNodeLocation configNodeLocation) {
    this();
    this.configNodeLocation = configNodeLocation;
  }

  public TConfigNodeLocation getConfigNodeLocation() {
    return configNodeLocation;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.RemoveConfigNode.ordinal());

    ThriftConfigNodeSerDeUtils.serializeTConfigNodeLocation(configNodeLocation, buffer);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    configNodeLocation = ThriftConfigNodeSerDeUtils.deserializeTConfigNodeLocation(buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(configNodeLocation);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RemoveConfigNodeReq that = (RemoveConfigNodeReq) obj;
    return configNodeLocation.equals(that.configNodeLocation);
  }
}
