package cn.edu.tsinghua.tsfile.encoding.encoder;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;

/**
 * Gorilla encoding. For more information about how it works, 
 * please see http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 */
public abstract class GorillaEncoder extends Encoder{
	// flag to indicate whether the first value is saved
	protected boolean flag;
	protected int leadingZeroNum, tailingZeroNum;
	// 8-bit buffer of bits to write out
	protected byte buffer;
	// number of bits remaining in buffer
	protected int numberLeftInBuffer;
	
	public GorillaEncoder() {
		super(TSEncoding.GORILLA);
		this.flag = false;
	}

	protected void writeBit(boolean b, ByteArrayOutputStream out){
		// add bit to buffer
        buffer <<= 1;
        if (b) buffer |= 1;

        // if buffer is full (8 bits), write out as a single byte
        numberLeftInBuffer++;
        if (numberLeftInBuffer == 8) clearBuffer(out);
	}
	
	protected void writeBit(int i, ByteArrayOutputStream out){
		if(i == 0){
			writeBit(false, out);
		} else{
			writeBit(true, out);
		}
	}
	
	protected void writeBit(long i, ByteArrayOutputStream out){
		if(i == 0){
			writeBit(false, out);
		} else{
			writeBit(true, out);
		}
	}
	
	protected void clearBuffer(ByteArrayOutputStream out){
		if (numberLeftInBuffer == 0) return;
        if (numberLeftInBuffer > 0) buffer <<= (8 - numberLeftInBuffer);
        out.write(buffer);
        numberLeftInBuffer = 0;
        buffer = 0;
	}
	
	protected void reset(){
		this.flag = false;
		this.numberLeftInBuffer = 0;
		this.buffer = 0;
	}
}
