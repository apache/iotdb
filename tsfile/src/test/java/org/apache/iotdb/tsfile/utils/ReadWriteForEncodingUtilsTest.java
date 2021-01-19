package org.apache.iotdb.tsfile.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.ByteBuffer;
import org.junit.Test;

public class ReadWriteForEncodingUtilsTest {

  @Test
  public void getUnsignedVarIntTest() {
    byte[] bytes = ReadWriteForEncodingUtils.getUnsignedVarInt(1);
    assertEquals(1, bytes.length);
    assertEquals(1, ReadWriteForEncodingUtils.readUnsignedVarInt(ByteBuffer.wrap(bytes)));
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);
    assertEquals(1, ReadWriteForEncodingUtils.writeVarInt(-1, byteBuffer));
    byteBuffer.flip();
    assertNotEquals(-1, ReadWriteForEncodingUtils.readUnsignedVarInt(byteBuffer));
  }

  @Test
  public void readAndWriteVarIntTest() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(5);
    // positive num
    assertEquals(1, ReadWriteForEncodingUtils.writeVarInt(9, byteBuffer));
    byteBuffer.flip();
    assertEquals(9, ReadWriteForEncodingUtils.readVarInt(byteBuffer));

    byteBuffer.flip();
    // negative num
    assertEquals(1, ReadWriteForEncodingUtils.writeVarInt(-1, byteBuffer));
    byteBuffer.flip();
    assertEquals(-1, ReadWriteForEncodingUtils.readVarInt(byteBuffer));
  }
}
