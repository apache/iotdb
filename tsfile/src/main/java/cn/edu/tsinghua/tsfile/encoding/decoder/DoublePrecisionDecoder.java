package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

/**
 * Decoder for value value using gorilla
 */
public class DoublePrecisionDecoder extends GorillaDecoder{
	private static final Logger LOGGER = LoggerFactory.getLogger(DoublePrecisionDecoder.class);
	private long preValue;
	
	public DoublePrecisionDecoder() {
	}
	
	@Override
	public double readDouble(InputStream in) {
		if (!flag) {
			flag = true;
			try {
		        int[] buf = new int[8];
		        for (int i = 0; i < 8; i++)
	                buf[i] = in.read();
		        long res = 0L;
		        for (int i = 0; i < 8; i++) {
		            res += ((long) buf[i] << (i * 8));
		        }
		        preValue = res;
		        double tmp = Double.longBitsToDouble(preValue);
				leadingZeroNum = Long.numberOfLeadingZeros(preValue);
				tailingZeroNum = Long.numberOfTrailingZeros(preValue);
				fillBuffer(in);
				getNextValue(in);
				return tmp;
			} catch (IOException e) {
				LOGGER.error("DoublePrecisionDecoder cannot read first double number because: {}", e.getMessage());
			}
		} else {
			try {
				double tmp = Double.longBitsToDouble(preValue);
				getNextValue(in);
				return tmp;
			} catch (IOException e) {
				LOGGER.error("DoublePrecisionDecoder cannot read following double number because: {}", e.getMessage());
			}
		}
		return Double.NaN;
	}
	
	/**
	 * check whether there is any value to encode left
	 * 
	 * @param in stream to read
	 * @throws IOException cannot read from stream
	 */
	private void getNextValue(InputStream in) throws IOException {
		nextFlag1 = readBit(in);
		// case: '0'
		if (!nextFlag1) {
			return;
		}
		nextFlag2 = readBit(in);
		
		if (!nextFlag2) {
			// case: '10'
			long tmp = 0;
			for (int i = 0; i < TSFileConfig.DOUBLE_LENGTH - leadingZeroNum - tailingZeroNum; i++) {
				long bit = readBit(in) ? 1 : 0;
				tmp |= (bit << (TSFileConfig.DOUBLE_LENGTH - 1 - leadingZeroNum - i));
			}
			tmp ^= preValue;
			preValue = tmp;
		} else {
			// case: '11'
			int leadingZeroNumTmp = readIntFromStream(in, TSFileConfig.DOUBLE_LEADING_ZERO_LENGTH);
			int lenTmp = readIntFromStream(in, TSFileConfig.DOUBLE_VALUE_LENGTH);
			long tmp = readLongFromStream(in, lenTmp);
			tmp <<= (TSFileConfig.DOUBLE_LENGTH - leadingZeroNumTmp - lenTmp);
			tmp ^= preValue;
			preValue = tmp;
		}
		leadingZeroNum = Long.numberOfLeadingZeros(preValue);
		tailingZeroNum = Long.numberOfTrailingZeros(preValue);
		if(Double.isNaN(Double.longBitsToDouble(preValue))){
			isEnd = true;
		}
	}
}
