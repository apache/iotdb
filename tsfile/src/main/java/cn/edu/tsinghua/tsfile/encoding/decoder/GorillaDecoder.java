package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

public abstract class GorillaDecoder extends Decoder {
	private static final Logger LOGGER = LoggerFactory.getLogger(GorillaDecoder.class);
	protected static final int EOF = -1;
	// flag to indicate whether the first value is read from stream
	protected boolean flag;
	protected int leadingZeroNum, tailingZeroNum;
	protected boolean isEnd;
	// 8-bit buffer of bits to write out
	protected int buffer;
	// number of bits remaining in buffer
	protected int numberLeftInBuffer;
	
	protected boolean nextFlag1;
	protected boolean nextFlag2;

	public GorillaDecoder() {
		super(TSEncoding.GORILLA);
		this.flag = false;
		this.isEnd = false;
	}

	@Override
	public boolean hasNext(InputStream in) throws IOException {
		if (in.available() > 0 || !isEnd) {
			return true;
		}
		return false;
	}

	protected boolean isEmpty() {
        return buffer == EOF;
    }
	
	protected boolean readBit(InputStream in) throws IOException {
		if(numberLeftInBuffer == 0 && !isEnd){
			fillBuffer(in);
		}
		if (isEmpty()) throw new IOException("Reading from empty input stream");
        numberLeftInBuffer--;
        return ((buffer >> numberLeftInBuffer) & 1) == 1;
    }
	
	/**
	 * read one byte and save in buffer
	 * @param in stream to read
	 */
	protected void fillBuffer(InputStream in) {
        try {
            buffer = in.read();
            numberLeftInBuffer = 8;
        } catch (IOException e) {
        		LOGGER.error("Failed to fill a new buffer, because {}",e.getMessage());
            buffer = EOF;
            numberLeftInBuffer = -1;
        }
    }
	
	/**
	 * read some bits and convert them to a int value
	 * @param in stream to read
	 * @param len number of bit to read
	 * @return converted int value
	 * @throws IOException cannot read from stream
	 */
	protected int readIntFromStream(InputStream in, int len) throws IOException{
		int num = 0;
		for (int i = 0; i < len; i++) {
			int bit = readBit(in) ? 1 : 0;
			num |= bit << (len - 1 - i);
		}
		return num;
	}
	
	/**
	 * read some bits and convert them to a long value
	 * @param in stream to read
	 * @param len number of bit to read
	 * @return converted long value
	 * @throws IOException cannot read from stream
	 */
	protected long readLongFromStream(InputStream in, int len) throws IOException{
		long num = 0;
		for (int i = 0; i < len; i++) {
			long bit = (long)(readBit(in) ? 1 : 0);
			num |= bit << (len - 1 - i);
		}
		return num;
	}
}
