package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.encoding.bitpacking.IntPacker;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TextRleDecoder extends RleDecoder{

    private static final Logger logger = LoggerFactory.getLogger(IntRleDecoder.class);

    /** current value for rle repeated value. */
    private int currentValue;

    private Binary  currentBinaryValue;

    /** buffer to save all values in group using bit-packing. */
    private int[] currentBuffer;

    /** packer for unpacking int values. */
    private IntPacker packer;

    public TextRleDecoder(){
        super();
        currentValue = 0;
        currentBinaryValue = new Binary("");
    }

    @Override
    public boolean readBoolean(ByteBuffer buffer) {
        boolean r = !this.readBinary(buffer).equals(0);
        return r;
    }

    @Override
    public Binary readBinary(ByteBuffer buffer) {
        if (!isLengthAndBitWidthReaded) {
            // start to read a new rle+bit-packing pattern
            readLengthAndBitWidth(buffer);
        }
        if (currentCount == 0) {
            try {
                readNext();
            } catch (IOException e) {
                logger.error(
                        "tsfile-encoding IntRleDecoder: error occurs when reading all encoding number,"
                                + " length is {}, bit width is {}",
                        length,
                        bitWidth,
                        e);
            }
        }
        --currentCount;
        int result;
        switch (mode) {
            case RLE:
                result = currentValue;
                break;
            case BIT_PACKED:
                result = currentBuffer[bitPackingNum - currentCount - 1];
                break;
            default:
                throw new TsFileDecodingException(
                        String.format("tsfile-encoding IntRleDecoder: not a valid mode %s", mode));
        }

        if (!hasNextPackage()) {
            isLengthAndBitWidthReaded = false;
        }
        byte[] tmp_bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            int offset = 24 - i * 8;
            tmp_bytes[i] = (byte) ((result >>> offset) & 0xFF);
        }

        Binary result_binary = new Binary(tmp_bytes);
        return result_binary;
    }


    @Override
    protected void initPacker() {
        packer = new IntPacker(bitWidth);
    }

    @Override
    protected void readNumberInRle() throws IOException {
        currentValue =
                ReadWriteForEncodingUtils.readIntLittleEndianPaddedOnBitWidth(byteCache, bitWidth);
    }

    @Override
    protected void readBitPackingBuffer(int bitPackedGroupCount, int lastBitPackedNum) throws IOException {
        currentBuffer = new int[bitPackedGroupCount * TSFileConfig.RLE_MIN_REPEATED_NUM];
        byte[] bytes = new byte[bitPackedGroupCount * bitWidth];
        int bytesToRead = bitPackedGroupCount * bitWidth;
        bytesToRead = Math.min(bytesToRead, byteCache.remaining());
        byteCache.get(bytes, 0, bytesToRead);

        // save all int values in currentBuffer
        packer.unpackAllValues(bytes, bytesToRead, currentBuffer);
    }
}
