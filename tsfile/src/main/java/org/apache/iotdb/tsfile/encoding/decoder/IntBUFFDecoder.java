package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IntBUFFDecoder extends Decoder {

  private boolean readMeta;
  private int minValue;
  private int countA, countB, n;

  private byte buffer = 0;
  private int bitsLeft = 0;

  public IntBUFFDecoder() {
    super(TSEncoding.BUFF);
    reset();
  }

  @Override
  public boolean hasNext(ByteBuffer in) throws IOException {
    if (!readMeta) readMeta(in);
    return n > 0;
  }

  @Override
  public int readInt(ByteBuffer in) {
    if (!readMeta) readMeta(in);
    int partA = readBits(in, countA);
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
      minValue = (int) readBits(in, Integer.SIZE);
    }
    readMeta = true;
  }

  protected int readBits(ByteBuffer in, int len) {
    int result = 0;
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
