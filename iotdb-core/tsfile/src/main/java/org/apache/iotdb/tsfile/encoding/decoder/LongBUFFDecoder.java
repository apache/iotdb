package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LongBUFFDecoder extends Decoder {

  private boolean readMeta;
  private long minValue;
  private int countA, countB, n;

  private byte buffer = 0;
  private int bitsLeft = 0;

  public LongBUFFDecoder() {
    super(TSEncoding.BUFF);
    reset();
  }

  @Override
  public boolean hasNext(ByteBuffer in) throws IOException {
    if (!readMeta) readMeta(in);
    return n > 0;
  }

  @Override
  public long readLong(ByteBuffer in) {
    if (!readMeta) readMeta(in);
    long partA = readBits(in, countA);
    n--;
    return minValue + partA;
  }

  @Override
  public void reset() {
    readMeta = false;

    buffer = 0;
    bitsLeft = 0;
  }

  private void readMeta(ByteBuffer in) {
    n = (int) readBits(in, Integer.SIZE);
    if (n > 0) {
      countA = (int) readBits(in, Integer.SIZE);
      countB = (int) readBits(in, Integer.SIZE);
      minValue = (int) readBits(in, Long.SIZE);
    }
    readMeta = true;
  }

  protected long readBits(ByteBuffer in, int len) {
    long result = 0;
    for (int i = 0; i < len; i++) {
      result <<= 1;
      if (readBit(in)) result |= 1;
    }
    return result;
  }

  /**
   * Reads the next bit and returns a boolean representing it.
   *
   * @return true if the next bit is 1, otherwise 0.
   */
  protected boolean readBit(ByteBuffer in) {
    flipByte(in);
    boolean bit = ((buffer >> (bitsLeft - 1)) & 1) == 1;
    bitsLeft--;
    return bit;
  }

  protected void flipByte(ByteBuffer in) {
    if (bitsLeft == 0) {
      buffer = in.get();
      bitsLeft = Byte.SIZE;
    }
  }
}
