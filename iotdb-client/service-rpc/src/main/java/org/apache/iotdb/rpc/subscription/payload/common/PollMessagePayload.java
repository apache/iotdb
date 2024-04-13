package org.apache.iotdb.rpc.subscription.payload.common;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class PollMessagePayload implements SubscriptionMessagePayload {

  private transient Set<String> topicNames = new HashSet<>();

  public PollMessagePayload() {}

  public PollMessagePayload(Set<String> topicNames) {
    this.topicNames = topicNames;
  }

  public Set<String> getTopicNames() {
    return topicNames;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.writeObjectSet(topicNames, stream);
  }

  @Override
  public SubscriptionMessagePayload deserialize(ByteBuffer buffer) {
    topicNames = ReadWriteIOUtils.readObjectSet(buffer);
    return this;
  }
}
