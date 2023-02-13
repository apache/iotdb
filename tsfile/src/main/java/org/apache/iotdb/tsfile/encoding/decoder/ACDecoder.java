package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private long Rbit = 32;
  private long bytemax = (1 << 8) - 1;
  private long Rmax = ((long) 1) << (Rbit + 8);
  private long Rmin = ((long) 1) << Rbit;
  private long L, R, V;
  private int readcnt;

  private int ByteToInt(byte b) {
    int res = (int) b;
    if (res < 0) res += 1 << 8;
    return res;
  }

  private long decoder_target_epc(long T) {
    long code = V;
    R /= T;
    if (code < L) {
      return (code + Rmax - L) / R;
    } else return (code - L) / R;
  }

  private void decode_epc(long cf, long f, ByteBuffer buffer) {
    L += cf * R;
    R *= f;
    while (R <= Rmin) {
      L = (L << 8) & (Rmax - 1);
      R <<= 8;
      if (readcnt < length) {
        V = ((V << 8) | ByteToInt(getByte(buffer))) & (Rmax - 1);
        readcnt++;
      } else V = (V << 8) & (Rmax - 1);
    }
  }

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
    n = getInt(buffer);
    for (int i = 0; i <= 256; i++) frequency[i] = getInt(buffer);

    what = new int[n];
    for (int i = 0; i <= 256; i++) {
      for (int j = (i == 0 ? 0 : frequency[i - 1]); j < frequency[i]; j++) what[j] = i;
    }

    length = getInt(buffer);
    readcnt = 0;
    L = 0;
    R = 1;
    tmp = new ArrayList<Byte>();
    decode_epc(0, 1, buffer);
    for (int i = 0; i < n; i++) {
      int x = what[(int) decoder_target_epc(n)];
      if (x == 256) {
        records.add(ListToBinary(tmp));
        tmp = new ArrayList<Byte>();
      } else {
        tmp.add((byte) (x));
      }
      decode_epc(
          (x == 0 ? 0 : frequency[x - 1]), frequency[x] - (x == 0 ? 0 : frequency[x - 1]), buffer);
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
