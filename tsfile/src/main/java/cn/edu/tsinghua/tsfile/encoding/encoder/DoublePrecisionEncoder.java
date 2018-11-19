package cn.edu.tsinghua.tsfile.encoding.encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

/**
 * Encoder for int value using gorilla encoding
 *
 */
public class DoublePrecisionEncoder extends GorillaEncoder {
	private long preValue;

	public DoublePrecisionEncoder() {
	}

	@Override
	public void encode(double value, ByteArrayOutputStream out) throws IOException {
		if (!flag) {
			// case: write first 8 byte value without any encoding
			flag = true;
			preValue = Double.doubleToLongBits(value);
			leadingZeroNum = Long.numberOfLeadingZeros(preValue);
			tailingZeroNum = Long.numberOfTrailingZeros(preValue);
			byte[] bufferBig = new byte[8];
			byte[] bufferLittle = new byte[8];

			for (int i = 0; i < 8; i++) {
				bufferLittle[i] = (byte) (((preValue) >> (i * 8)) & 0xFF);
				bufferBig[8 - i - 1] = (byte) (((preValue) >> (i * 8)) & 0xFF);
			}
			out.write(bufferLittle);
		} else {
			long nextValue = Double.doubleToLongBits(value);
			long tmp = nextValue ^ preValue;
			if (tmp == 0) {
				// case: write '0'
				writeBit(false, out);
			} else {
				int leadingZeroNumTmp = Long.numberOfLeadingZeros(tmp);
				int tailingZeroNumTmp = Long.numberOfTrailingZeros(tmp);
				if (leadingZeroNumTmp >= leadingZeroNum && tailingZeroNumTmp >= tailingZeroNum) {
					// case: write '10' and effective bits without first leadingZeroNum '0' and last tailingZeroNum '0'
					writeBit(true, out);
					writeBit(false, out);
					writeBits(tmp, out, TSFileConfig.DOUBLE_LENGTH - 1 - leadingZeroNum, tailingZeroNum);
				} else {
					// case: write '11', leading zero num of value, effective bits len and effective bit value
					writeBit(true, out);
					writeBit(true, out);
					writeBits(leadingZeroNumTmp, out, TSFileConfig.DOUBLE_LEADING_ZERO_LENGTH - 1, 0);
					writeBits(TSFileConfig.DOUBLE_LENGTH - leadingZeroNumTmp - tailingZeroNumTmp, out, TSFileConfig.DOUBLE_VALUE_LENGTH - 1, 0);
					writeBits(tmp, out, TSFileConfig.DOUBLE_LENGTH - 1 - leadingZeroNumTmp, tailingZeroNumTmp);
				}
			}
			preValue = nextValue;
			leadingZeroNum = Long.numberOfLeadingZeros(preValue);
			tailingZeroNum = Long.numberOfTrailingZeros(preValue);
		}
	}

	private void writeBits(long num, ByteArrayOutputStream out, int start, int end) {
		for (int i = start; i >= end; i--) {
			long bit = num & (1L << i);
			writeBit(bit, out);
		}
	}
	
	@Override
	public void flush(ByteArrayOutputStream out) throws IOException {
		encode(Double.NaN, out);
		clearBuffer(out);
		reset();
	}
	
    @Override
    public int getOneItemMaxSize() {
    		// case '11'
		// 2bit + 6bit + 7bit + 64bit = 79bit 
        return 10;
    }

    @Override
    public long getMaxByteSize() {
		// max(first 8 byte, case '11' 2bit + 6bit + 7bit + 64bit = 79bit ) + NaN(2bit + 6bit + 7bit + 64bit = 79bit) = 158bit
        return 20;
    }
}
