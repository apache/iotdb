package org.apache.iotdb.rpc.subscription.payload.common;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

public class TabletsMessagePayload implements SubscriptionRawMessagePayload {

  private transient List<Tablet> tablets = new ArrayList<>();

  public TabletsMessagePayload() {}

  public TabletsMessagePayload(List<Tablet> tablets) {
    this.tablets = tablets;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tablets.size(), stream);
    for (final Tablet tablet : tablets) {
      tablet.serialize(stream);
    }
  }

  @Override
  public SubscriptionRawMessagePayload deserialize(ByteBuffer buffer) {
    final List<Tablet> tablets = new ArrayList<>();
    final int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      tablets.add(Tablet.deserialize(buffer));
    }
    this.tablets = tablets;
    return this;
  }
}
