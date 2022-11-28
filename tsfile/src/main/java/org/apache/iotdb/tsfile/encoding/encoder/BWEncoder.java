package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.encoding.SA_IS.SA_IS;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class BWEncoder extends Encoder {

  private static final Logger logger = LoggerFactory.getLogger(BWEncoder.class);

  private MTFEncoder mymtf;
  private int maxRecordLength;
  private List<Binary> li;
  private List<Binary> records;
  private int n, tot;
  private int[] s;
  private SA_IS sais;
  private int numberLeftInBuffer = 0;
  private byte byteBuffer;

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

  BWEncoder() {
    super(TSEncoding.BW);
    mymtf = new MTFEncoder();
    maxRecordLength = 0;
    li = new ArrayList<Binary>();
    records = new ArrayList<Binary>();
    n = 0;
    tot = 0;
    sais = new SA_IS();
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    maxRecordLength = Math.max(maxRecordLength, value.getLength());
    n += value.getLength() + 1;
    li.add(value);
  }

  private List<Byte> tmp;

  @Override
  public void flush(ByteArrayOutputStream out) {
    s = new int[n];
    int idx = 0;
    for (Binary str : li) {
      for (int i = 0; i < str.getLength(); i++) s[idx++] = ByteToInt(str.getValues()[i]) + 1;
      s[idx++] = 257;
    }
    s[n - 1] = 0;
    s = sais.solve(s);
    tmp = new ArrayList<Byte>();
    for (int i = 0; i < s.length; i++) {
      if (s[i] == 0) {
        logger.info("{}:{}", records.size(), tmp.size());
        writeInt(records.size(), out);
        tot += 32;
        writeInt(tmp.size(), out);
        tot += 32;
        tmp.add((byte) 0);
      } else if (s[i] == 257) {
        records.add(new Binary(ListToArray(tmp)));
        tmp = new ArrayList<Byte>();
      } else {
        tmp.add((byte) (s[i] - 1));
      }
    }
    records.add(new Binary(ListToArray(tmp)));
    tmp = new ArrayList<Byte>();
    for (Binary rec : records) mymtf.encode(rec, out);
    mymtf.flush(out);
    maxRecordLength = 0;
    n = 0;
    li.clear();
  }

  @Override
  public int getOneItemMaxSize() {
    return maxRecordLength;
  }

  @Override
  public long getMaxByteSize() {
    return tot + mymtf.getMaxByteSize();
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
    numberLeftInBuffer = 0;
    byteBuffer = 0;
  }

  private void writeInt(int val, ByteArrayOutputStream out) {
    for (int i = 31; i >= 0; i--) {
      if ((val & (1 << i)) > 0) writeBit(true, out);
      else writeBit(false, out);
    }
  }

  //   private void writeByte(byte val, ByteArrayOutputStream out) {
  //     for (int i = 7; i >= 0; i--) {
  //       if ((val & (1 << i)) > 0) writeBit(true, out);
  //       else writeBit(false, out);
  //     }
  //   }
}
