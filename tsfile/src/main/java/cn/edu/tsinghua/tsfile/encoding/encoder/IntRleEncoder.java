package cn.edu.tsinghua.tsfile.encoding.encoder;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.bitpacking.IntPacker;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;


/**
 * Encoder for int value using rle or bit-packing
 */
public class IntRleEncoder extends RleEncoder<Integer> {

    /**
     * Packer for packing int value
     */
    private IntPacker packer;

    public IntRleEncoder(EndianType endianType) {
        super(endianType);
        bufferedValues = new Integer[config.RLE_MIN_REPEATED_NUM];
        preValue = 0;
        values = new ArrayList<Integer>();
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
        values.add(value);
    }
    
	@Override
	public void encode(boolean value, ByteArrayOutputStream out) {
		if (value) {
			this.encode(1, out);
		} else {
			this.encode(0, out);
		}
	}

    /**
     * write all values buffered in cache to OutputStream
     *
     * @param out - byteArrayOutputStream
     * @throws IOException cannot flush to OutputStream
     */
    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // we get bit width after receiving all data
        this.bitWidth = ReadWriteStreamUtils.getIntMaxBitWidth(values);
        packer = new IntPacker(bitWidth);
        for (Integer value : values) {
            encodeValue(value);
        }
        super.flush(out);
    }

    @Override
    protected void reset() {
        super.reset();
        preValue = 0;
    }

    /**
     * write bytes to OutputStream using rle
     * rle format: [header][value]
     */
    @Override
    protected void writeRleRun() throws IOException {
        endPreviousBitPackedRun(config.RLE_MIN_REPEATED_NUM);
        ReadWriteStreamUtils.writeUnsignedVarInt(repeatCount << 1, byteCache);
        ReadWriteStreamUtils.writeIntLittleEndianPaddedOnBitWidth(preValue, byteCache, bitWidth);
        repeatCount = 0;
        numBufferedValues = 0;
    }

    @Override
    protected void clearBuffer() {

        for (int i = numBufferedValues; i < config.RLE_MIN_REPEATED_NUM; i++) {
            bufferedValues[i] = 0;
        }
    }

    @Override
    protected void convertBuffer() {
        byte[] bytes = new byte[bitWidth];

        int[] tmpBuffer = new int[config.RLE_MIN_REPEATED_NUM];
        for (int i = 0; i < config.RLE_MIN_REPEATED_NUM; i++) {
            tmpBuffer[i] = (int) bufferedValues[i];
        }
        packer.pack8Values(tmpBuffer, 0, bytes);
        // we'll not write bit-packing group to OutputStream immediately
        // we buffer them in list
        bytesBuffer.add(bytes);
    }

    @Override
    public int getOneItemMaxSize() {
        // The meaning of 45 is:
        // 4 + 4 + max(4+4,1 + 4 + 4 * 8)
        // length + bitwidth + max(rle-header + num, bit-header + lastNum + 8packer)
        return 45;
    }

    @Override
    public long getMaxByteSize() {
        if (values == null) {
            return 0;
        }
        // try to caculate max value
        int groupNum = (values.size() / 8 + 1) / 63 + 1;
        return 8 + groupNum * 5 + values.size() * 4;
    }
}