package org.apache.iotdb.rpc.subscription.payload.common;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TsFileInfoMessagePayload implements SubscriptionRawMessagePayload {

  protected transient String fileName;

  public TsFileInfoMessagePayload() {}

  public TsFileInfoMessagePayload(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(fileName, stream);
  }

  @Override
  public SubscriptionRawMessagePayload deserialize(ByteBuffer buffer) {
    this.fileName = ReadWriteIOUtils.readString(buffer);
    return this;
  }
}
