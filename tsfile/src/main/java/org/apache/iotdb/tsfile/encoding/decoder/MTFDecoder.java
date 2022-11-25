package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.BIT.BIT;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class MTFDecoder extends Decoder {

  private static final int Sigma = 256;

  private HuffmanDecoder myhuffman;
  private List<Binary> li;
  private Queue<Binary> records;
  private BIT bit;
  private int n, idx;
  private int[] last, ans;

  MTFDecoder() {
    super(TSEncoding.MTF);
    myhuffman = new HuffmanDecoder();
    li = new ArrayList<Binary>();
    records = new LinkedList<>();
    reset();
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
    ans[idx] = c;
    return res;
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    if (records.isEmpty()) {
      reset();
      load(buffer);
    }
    return records.poll();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    return ((!records.isEmpty()) || buffer.hasRemaining());
  }

  @Override
  public void reset() {
    myhuffman.reset();
    li.clear();
    records.clear();
    bit = null;
    n = Sigma;
    idx = 0;
    last = new int[Sigma];
    ans = null;
    return;
  }

  private void load(ByteBuffer buffer) {
    while (myhuffman.hasNext(buffer)) {
      Binary rec = myhuffman.readBinary(buffer);
      n += rec.getLength();
      li.add(rec);
    }
    bit = new BIT(n);
    ans = new int[n + 1];
    for (int i = Sigma - 1; i >= 0; i--) show(i);
    for (Binary str : li) {
      byte[] res = new byte[str.getLength()];
      for (int i = 0; i < str.getLength(); i++) {
        res[i] = (byte) ans[bit.find(ByteToInt(str.getValues()[i]) + 1)];
        show(ByteToInt(res[i]));
      }
      records.add(new Binary(res));
    }
  }
}
