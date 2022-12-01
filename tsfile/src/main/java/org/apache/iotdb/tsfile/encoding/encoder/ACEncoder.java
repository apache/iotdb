package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class ACEncoder extends Encoder {
  private static final Logger logger = LoggerFactory.getLogger(ACEncoder.class);

  private int n, m;
  private int[] frequency;
  private byte byteBuffer;
  private int numberLeftInBuffer = 0;
  private int maxRecordLength;
  private int totLength;
  private List<Binary> records;
  private List<Byte> tmp;

  private String trans(BigInteger a, int n) {
    String s = a.toString(2);
    int m = s.length();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n - m; i++) sb.append("0");
    sb.append(s);
    return sb.toString();
  }

  private int lcp(String s, String t) {
    for (int i = 0; i < s.length() && i < t.length(); i++) {
      if (s.charAt(i) != t.charAt(i)) return i;
    }
    return Math.min(s.length(), t.length());
  }

  private int ByteToInt(byte b) {
    int res = (int) b;
    if (res < 0) res += 1 << 8;
    return res;
  }

  private byte[] ListToArray(List<Byte> a) {
    byte[] b = new byte[a.size()];
    for (int i = 0; i < a.size(); i++) {
      b[i] = a.get(i);
    }
    return b;
  }

  public ACEncoder() {
    super(TSEncoding.AC);
    frequency = new int[257];
    records = new ArrayList<Binary>();
    reset();
  }

  private void reset() {
    maxRecordLength = 0;
    totLength = 0;
    records.clear();
    for (int i = 0; i <= 256; i++) frequency[i] = 0;
    n = 0;
    tmp = new ArrayList<Byte>();
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    maxRecordLength = Math.max(maxRecordLength, value.getLength());
    records.add(value);
    for (int i = 0; i < value.getLength(); i++) {
      frequency[value.getValues()[i]]++;
      n++;
    }
    frequency[256]++;
    n++;
    return;
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    BigInteger C = new BigInteger("0"), A = new BigInteger("1");
    int[] what = new int[n];
    int idx = 0, lgm, exp = 0;
    for (int i = 0; i <= 256; i++) for (int j = 0; j < frequency[i]; j++) what[idx++] = i;
    m = 1;
    lgm = 0;
    while (m < n) {
      m <<= 1;
      lgm++;
    }
    for (int i = 0; i < m - n; i++) frequency[what[(int) (Math.random() * n)]]++;
    for (int i = 1; i <= 256; i++) frequency[i] += frequency[i - 1];
    for (Binary value : records) {
      for (int i = 0; i < value.getLength(); i++) {
        int x = ByteToInt(value.getValues()[i]);
        logger.info("common!aoao:{}", x);
        C =
            C.multiply(BigInteger.valueOf(m))
                .add(A.multiply(BigInteger.valueOf(x == 0 ? 0 : frequency[x - 1])));
        A = A.multiply(BigInteger.valueOf(frequency[x] - (x == 0 ? 0 : frequency[x - 1])));
        exp += lgm;
      }
      int x = 256;
      logger.info("common!aoao:{}", x);
      C =
          C.multiply(BigInteger.valueOf(m))
              .add(A.multiply(BigInteger.valueOf(x == 0 ? 0 : frequency[x - 1])));
      A = A.multiply(BigInteger.valueOf(frequency[x] - (x == 0 ? 0 : frequency[x - 1])));
      exp += lgm;
    }
    String CS = trans(C, exp), AS = trans(C.add(A), exp);
    int len = lcp(CS, AS);
    exp -= len;
    for (int j = 0; j < len; j++) tmp.add((byte) (CS.charAt(j) == '0' ? 0 : 1));
    tmp.add((byte) 1);
    writeInt(n, out);
    writeInt(m, out);
    logger.info("n:OK!{}", n);
    for (int i = 0; i <= 256; i++) writeInt(frequency[i], out);
    byte[] tmp2 = ListToArray(tmp);
    writeInt(tmp2.length, out);
    for (int i = 0; i < tmp2.length; i++) {
      writeBit(tmp2[i] == 0 ? false : true, out);
    }
    reset();
    clearBuffer(out);
  }

  @Override
  public int getOneItemMaxSize() {
    return maxRecordLength;
  }

  @Override
  public long getMaxByteSize() {
    return totLength;
  }

  protected void writeBit(boolean b, ByteArrayOutputStream out) {
    byteBuffer <<= 1;
    if (b) {
      byteBuffer |= 1;
    }

    numberLeftInBuffer++;
    if (numberLeftInBuffer == 8) {
      clearBuffer(out);
    }
  }

  protected void clearBuffer(ByteArrayOutputStream out) {
    if (numberLeftInBuffer == 0) return;
    if (numberLeftInBuffer > 0) byteBuffer <<= (8 - numberLeftInBuffer);
    out.write(byteBuffer);
    totLength++;
    numberLeftInBuffer = 0;
    byteBuffer = 0;
  }

  private void writeInt(int val, ByteArrayOutputStream out) {
    for (int i = 31; i >= 0; i--) {
      if ((val & (1 << i)) > 0) writeBit(true, out);
      else writeBit(false, out);
    }
  }

  private void writeByte(byte val, ByteArrayOutputStream out) {
    for (int i = 7; i >= 0; i--) {
      if ((val & (1 << i)) > 0) writeBit(true, out);
      else writeBit(false, out);
    }
  }
}
