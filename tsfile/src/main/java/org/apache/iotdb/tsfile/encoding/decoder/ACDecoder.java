package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ACDecoder extends Decoder {
  private static final Logger logger = LoggerFactory.getLogger(ACDecoder.class);
  private int numberLeftInBuffer;
  private byte byteBuffer;
  private int n, m;
  private int[] frequency;
  private int[] what;
  private Queue<Binary> records;
  private int length;
  private List<Byte> tmp;

  public ACDecoder() {
    super(TSEncoding.AC);
    frequency = new int[257];
    records = new LinkedList<>();
    reset();
  }

  private Binary ListToBinary(List<Byte> tmp) {
    byte[] tmp2 = new byte[tmp.size()];
    for (int j = 0; j < tmp.size(); j++) tmp2[j] = (byte) tmp.get(j);
    return new Binary(tmp2);
  }

  public void reset() {
    records.clear();
    n = 0;
    m = 0;
    for (int i = 0; i <= 256; i++) frequency[i] = 0;
    return;
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    if (records.isEmpty()) {
      reset();
      load(buffer);
      clearBuffer(buffer);
    }
    return records.poll();
  }

  public boolean hasNext(ByteBuffer buffer) {
    return ((!records.isEmpty()) || buffer.hasRemaining());
  }

  private void load(ByteBuffer buffer) {
    logger.info("Yes!");
    n = getInt(buffer);
    m = getInt(buffer);
    logger.info("OK!!!n:{}", n);
    for (int i = 0; i <= 256; i++) frequency[i] = getInt(buffer);
    length = getInt(buffer);
    int exp = length;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) sb.append(readbit(buffer) == 0 ? '0' : '1');
    tmp = new ArrayList<Byte>();
    // what = new int[n];
    // for (int i = 0; i <= 256; i++)
    //   for (int j = (i == 0 ? m : frequency[i - 1]); j < frequency[i]; j++) what[j] = i;
    BigInteger C = new BigInteger(sb.toString(), 2), A = BigInteger.valueOf(1).shiftLeft(exp);
    for (int i = 0; i < n; i++) {
      int l = 0, r = 256, mid;
      while (l < r) {
        mid = (l + r + 1) >> 1;
        if (A.multiply(BigInteger.valueOf(mid == 0 ? 0 : frequency[mid - 1]))
                .compareTo(C.multiply(BigInteger.valueOf(m)))
            <= 0) l = mid;
        else r = mid - 1;
      }
      logger.info("hello,{}", l);
      if (l == 256) {
        records.add(ListToBinary(tmp));
        tmp = new ArrayList<Byte>();
      } else tmp.add((byte) l);
      C =
          C.multiply(BigInteger.valueOf(m))
              .subtract(A.multiply(BigInteger.valueOf(l == 0 ? 0 : frequency[l - 1])));
      A = A.multiply(BigInteger.valueOf(frequency[l] - (l == 0 ? 0 : frequency[l - 1])));
    }
  }

  private int getInt(ByteBuffer buffer) {
    int val = 0;
    for (int i = 31; i >= 0; i--) {
      val |= (readbit(buffer) << i);
    }
    return val;
  }

  private byte getByte(ByteBuffer buffer) {
    byte val = 0;
    for (int i = 7; i >= 0; i--) {
      val |= (readbit(buffer) << i);
    }
    return val;
  }

  private int readbit(ByteBuffer buffer) {
    if (numberLeftInBuffer == 0) {
      loadBuffer(buffer);
      numberLeftInBuffer = 8;
    }
    int top = ((byteBuffer >> 7) & 1);
    byteBuffer <<= 1;
    numberLeftInBuffer--;
    return top;
  }

  private void loadBuffer(ByteBuffer buffer) {
    byteBuffer = buffer.get();
  }

  private void clearBuffer(ByteBuffer buffer) {
    while (numberLeftInBuffer > 0) {
      readbit(buffer);
    }
  }
}
