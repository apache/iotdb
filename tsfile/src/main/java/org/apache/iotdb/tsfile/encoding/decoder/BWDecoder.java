package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.SA_IS.SA_IS_solve;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BWDecoder extends Decoder {
  private static final Logger logger = LoggerFactory.getLogger(BWDecoder.class);

  private MTFDecoder mymtf;
  private List<Binary> li;
  private Queue<Binary> records;
  private int cnt, cnt2;
  private int n;
  private int[] ans;
  private SA_IS_solve sais;
  private List<Byte> tmp;
  private int numberLeftInBuffer;
  private byte byteBuffer;

  private int ByteToInt(byte b) {
    int res = (int) b;
    if (res < 0) res += 1 << 8;
    return res;
  }

  private Binary ListToBinary(List<Byte> tmp) {
    byte[] tmp2 = new byte[tmp.size()];
    for (int j = 0; j < tmp.size(); j++) tmp2[j] = (byte) tmp.get(j);
    return new Binary(tmp2);
  }

  public BWDecoder() {
    super(TSEncoding.BW);
    mymtf = new MTFDecoder();
    li = new ArrayList<Binary>();
    records = new LinkedList<>();
    reset();
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
    mymtf.reset();
    li.clear();
    records.clear();
    n = -1;
    ans = null;
    sais = new SA_IS_solve();
    return;
  }

  private void load(ByteBuffer buffer) {
    cnt = getInt(buffer);
    cnt2 = getInt(buffer);
    logger.info("e:{}:{}", cnt, cnt2);
    int idx = 0;
    while (mymtf.hasNext(buffer)) {
      Binary rec = mymtf.readBinary(buffer);
      n += rec.getLength() + 1;
      li.add(rec);
    }
    ans = new int[n];
    for (int i = 0; i < li.size(); i++) {
      for (int j = 0; j < li.get(i).getLength(); j++) {
        if (i == cnt && j == cnt2) {
          ans[idx++] = 0;
        } else ans[idx++] = ByteToInt(li.get(i).getValues()[j]) + 1;
      }
      if (i != li.size() - 1) ans[idx++] = 257;
    }
    ans = sais.solve(ans);
    tmp = new ArrayList<Byte>();
    for (int i = 0; i < ans.length - 1; i++) {
      if (ans[i] == 257) {
        records.add(ListToBinary(tmp));
        tmp = new ArrayList<Byte>();
      } else {
        tmp.add((byte) (ans[i] - 1));
      }
    }
    records.add(ListToBinary(tmp));
    tmp = new ArrayList<Byte>();
  }

  private int getInt(ByteBuffer buffer) {
    int val = 0;
    for (int i = 31; i >= 0; i--) {
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
}
