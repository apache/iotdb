package org.apache.iotdb.rpc.subscription.payload.common;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PollTsFileMessagePayload implements SubscriptionMessagePayload {

  private transient String topicName;

  private transient String fileName;

  private transient long endWritingOffset;

  public String getTopicName() {
    return topicName;
  }

  public String getFileName() {
    return fileName;
  }

  public long getEndWritingOffset() {
    return endWritingOffset;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(topicName, stream);
    ReadWriteIOUtils.write(fileName, stream);
    ReadWriteIOUtils.write(endWritingOffset, stream);
  }

  @Override
  public SubscriptionMessagePayload deserialize(ByteBuffer buffer) {
    topicName = ReadWriteIOUtils.readString(buffer);
    fileName = ReadWriteIOUtils.readString(buffer);
    endWritingOffset = ReadWriteIOUtils.readLong(buffer);
    return this;
  }
}
