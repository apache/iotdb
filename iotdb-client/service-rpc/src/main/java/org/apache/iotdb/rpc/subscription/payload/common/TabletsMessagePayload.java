package org.apache.iotdb.rpc.subscription.payload.common;

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TabletsMessagePayload implements SubscriptionRawMessagePayload {

  private static final Logger LOGGER = LoggerFactory.getLogger(TabletsMessagePayload.class);

  protected transient List<Tablet> tablets = new ArrayList<>();

  private ByteBuffer byteBuffer; // serialized tablets

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

  //////////////////////////// serialization ////////////////////////////

  /** @return true -> byte buffer is not null */
  public boolean trySerialize() {
    if (Objects.isNull(byteBuffer)) {
      try {
        try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
            final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
          serialize(outputStream);
          byteBuffer =
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
        }
        return true;
      } catch (final IOException e) {
        LOGGER.warn(
            "Subscription: something unexpected happened when serializing Tablets, exception is {}",
            e.getMessage());
      }
      return false;
    }
    return true;
  }

  public void resetByteBuffer() {
    // maybe friendly for gc
    byteBuffer = null;
  }
}
