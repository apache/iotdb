package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
// import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class ACEncoder extends Encoder {
  // private static final Logger logger = LoggerFactory.getLogger(ACEncoder.class);

  private int n;
  private int[] frequency;
  private byte byteBuffer;
  private int numberLeftInBuffer = 0;
  private int maxRecordLength;
  private int totLength;
  private List<Binary> records;
  private List<Byte> tmp;

	private long Rbit=32;
	private long bytemax=(1<<8)-1;
  private long Rmax=((long)1)<<(Rbit+8);
  private long Rmin=((long)1)<<Rbit;

	private long L,H,R,d,nn;

  private void encode_epc(long cf,long f,long T) {
		R/=T;
		L+=cf*R;
		R*=f;
		while(R<=Rmin) {
			H=L+R-1;
			if(nn!=0) {
				if(H<=Rmax) {
					tmp.add((byte)d);
					// writeByte((byte)d, out);
					for(int i=1;i<=nn-1;i++) {
						tmp.add((byte)bytemax);
						// writeByte((byte)bytemax, out);
					}
					nn=0;
					L+=Rmax;
				}
				else if(L>=Rmax) {
					tmp.add((byte)(d+1));
					// writeByte((byte)(d+1), out);
					for(int i=1;i<=nn-1;i++) {
						tmp.add((byte)0);
						// writeByte((byte)0, out);
					}
					nn=0;
				}
				else {
					nn++;
					L=(L<<8)&(Rmax-1);
					R<<=8;
					continue;
				}
			}
			if(((L^H)>>Rbit)==0) {
				tmp.add((byte)(L>>Rbit));
				// writeByte((byte)(L>>Rbit), out);
			}
			else {
				L-=Rmax;
				d=L>>Rbit;
				nn=1;
			}
			L=((L<<8)&(Rmax-1))|(L&Rmax);
			R<<=8;
		}
	}

	private void finish_encode_epc() {
		if(nn!=0) {
			if(L<Rmax) {
				tmp.add((byte)d);
				// writeByte((byte)d, out);
				for(int i=1;i<=nn-1;i++) {
					tmp.add((byte)bytemax);
					// writeByte((byte)bytemax, out);
				}
			}
			else {
				tmp.add((byte)(d+1));
				// writeByte((byte)(d+1), out);
				for(int i=1;i<=nn-1;i++) {
					tmp.add((byte)0);
					// writeByte((byte)0, out);
				}
			}
		}
		long i=Rbit+8;
		do {
			i-=8;
			tmp.add((byte)(L>>i&bytemax));
			// writeByte((byte)(L>>i), out);
		}while(i>0);
	}

  private int ByteToInt(byte b) {
    int res = (int) b;
    if (res < 0) res += 1 << 8;
    return res;
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
    writeInt(n, out);
		for(int i=1;i<=256;i++) {
			frequency[i]+=frequency[i-1];
		}
		for(int i=0;i<=256;i++) {
			writeInt(frequency[i], out);
		}
		L=Rmax;R=Rmax;
		for(Binary rec:records) {
			for(int i=0;i<rec.getLength();i++) {
				int x=ByteToInt(rec.getValues()[i]);
				encode_epc((x==0?0:frequency[x-1]), frequency[x]-(x==0?0:frequency[x-1]), n);
			}
			int x=256;
			encode_epc((x==0?0:frequency[x-1]), frequency[x]-(x==0?0:frequency[x-1]), n);
		}
		finish_encode_epc();
		writeInt(tmp.size(), out);
		for(byte b:tmp) {
			writeByte(b, out);
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
