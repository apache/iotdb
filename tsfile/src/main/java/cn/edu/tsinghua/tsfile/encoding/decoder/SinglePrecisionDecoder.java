package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

/**
 * Decoder for value value using gorilla
 */
public class SinglePrecisionDecoder extends GorillaDecoder {
	private static final Logger LOGGER = LoggerFactory.getLogger(SinglePrecisionDecoder.class);
	private int preValue;

	public SinglePrecisionDecoder() {
	}

	@Override
	public float readFloat(InputStream in) {
		if (!flag) {
			flag = true;
			try {
				int ch1 = in.read();
				int ch2 = in.read();
				int ch3 = in.read();
				int ch4 = in.read();
				preValue = ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
				leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
				tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
				float tmp = Float.intBitsToFloat(preValue);
				fillBuffer(in);
				getNextValue(in);
				return tmp;
			} catch (IOException e) {
				LOGGER.error("SinglePrecisionDecoder cannot read first float number because: {}", e.getMessage());
			}
		} else {
			try {
				float tmp = Float.intBitsToFloat(preValue);
				getNextValue(in);
				return tmp;
			} catch (IOException e) {
				LOGGER.error("SinglePrecisionDecoder cannot read following float number because: {}", e.getMessage());
			}
		}
		return Float.NaN;
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
			int tmp = 0;
			for (int i = 0; i < TSFileConfig.FLOAT_LENGTH - leadingZeroNum - tailingZeroNum; i++) {
				int bit = readBit(in) ? 1 : 0;
				tmp |= bit << (TSFileConfig.FLOAT_LENGTH - 1 - leadingZeroNum - i);
			}
			tmp ^= preValue;
			preValue = tmp;
		} else {
			// case: '11'
			int leadingZeroNumTmp = readIntFromStream(in, TSFileConfig.FLAOT_LEADING_ZERO_LENGTH);
			int lenTmp = readIntFromStream(in, TSFileConfig.FLOAT_VALUE_LENGTH);
			int tmp = readIntFromStream(in, lenTmp);
			tmp <<= (TSFileConfig.FLOAT_LENGTH - leadingZeroNumTmp - lenTmp);
			tmp ^= preValue;
			preValue = tmp;
		}
		leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
		tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
		if(Float.isNaN(Float.intBitsToFloat(preValue))){
			isEnd = true;
		}
	}
}
