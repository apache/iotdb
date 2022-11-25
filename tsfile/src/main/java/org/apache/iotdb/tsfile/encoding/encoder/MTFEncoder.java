package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.encoding.BIT.BIT;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class MTFEncoder extends Encoder {

  private static final int Sigma = 256;

  private List<Binary> records;
  private HuffmanEncoder myhuffman;
  private BIT bit;
  private int maxRecordLength;
  private int n;
  private int[] last;
  private int idx;

  public MTFEncoder() {
    super(TSEncoding.MTF);
    myhuffman = new HuffmanEncoder();
    records = new ArrayList<Binary>();
    maxRecordLength = 0;
    n = Sigma;
    last = new int[Sigma];
    idx = 0;
  }

  private int ByteToInt(byte b) {
    int res = (int) b;
    if (res < 0) res += 1 << 8;
    return res;
  }

  private int show(int c) {
    int res = bit.ask(last[c] + 1);
    idx++;
    bit.add(last[c] + 1, 1);
    bit.add(idx + 1, -1);
    last[c] = idx;
    return res;
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    maxRecordLength = Math.max(maxRecordLength, value.getLength());
    n += value.getLength();
    records.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    bit = new BIT(n);
    for (int i = Sigma - 1; i >= 0; i--) show(i);
    for (Binary str : records) {
      byte[] res = new byte[str.getLength()];
      for (int i = 0; i < str.getLength(); i++) res[i] = (byte) show(ByteToInt(str.getValues()[i]));
      myhuffman.encode(new Binary(res), out);
    }
    myhuffman.flush(out);
  }

  @Override
  public int getOneItemMaxSize() {
    return maxRecordLength;
  }

  @Override
  public long getMaxByteSize() {
    return myhuffman.getMaxByteSize();
  }
}
