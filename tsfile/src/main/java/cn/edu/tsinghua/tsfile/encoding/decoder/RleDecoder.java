package cn.edu.tsinghua.tsfile.encoding.decoder;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.exception.TSFileDecodingException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

/**
 * Abstract class for all rle decoder. Decoding values according to
 * following grammar: {@code <length> <bitwidth> <encoded-data>}. For more
 * information about rle format, see RleEncoder
 */
public abstract class RleDecoder extends Decoder {
    // private static final Logger LOGGER = LoggerFactory.getLogger(RleDecoder.class);
    public EndianType endianType;
    protected TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    /**
     * mode to indicate current encoding type
     * 0 - RLE
     * 1 - BIT_PACKED
     */
    protected MODE mode;
    /**
     * bit width for bit-packing and rle to decode
     */
    protected int bitWidth;
    /**
     * number of data left for reading in current buffer
     */
    protected int currentCount;
    /**
     * how many bytes for all encoded data like [{@code <bitwidth> <encoded-data>}] in
     * inputstream
     */
    protected int length;
    /**
     * a flag to indicate whether current pattern is end. false - need to start
     * reading a new page true - current page isn't over
     */
    protected boolean isLengthAndBitWidthReaded;
    /**
     * buffer to save data format like [{@code <bitwidth> <encoded-data>}] for decoder
     */
    protected ByteArrayInputStream byteCache;
    /**
     * number of bit-packing group in which is saved in header
     */
    protected int bitPackingNum;

    public RleDecoder(EndianType endianType) {
        super(TSEncoding.RLE);
        this.endianType = endianType;
        currentCount = 0;
        isLengthAndBitWidthReaded = false;
        bitPackingNum = 0;
        byteCache = new ByteArrayInputStream(new byte[0]);
        // LOGGER.debug("tsfile-encoding RleDecoder: init rle decoder");
    }

    /**
     * get header for both rle and bit-packing current encode mode which is
     * saved in first bit of header
     * @return int value
     * @throws IOException cannot get header
     */
    public int getHeader() throws IOException {
        int header = ReadWriteStreamUtils.readUnsignedVarInt(byteCache);
        mode = (header & 1) == 0 ? MODE.RLE : MODE.BIT_PACKED;
        return header;
    }

    /**
     * get all encoded data according to mode
     *
     * @throws IOException cannot read next value
     */
    protected void readNext() throws IOException {
        int header = getHeader();
        switch (mode) {
            case RLE:
                currentCount = header >> 1;
                readNumberInRLE();
                break;
            case BIT_PACKED:
                int bitPackedGroupCount = header >> 1;
                // in last bit-packing group, there may be some useless value,
                // lastBitPackedNum indicates how many values is useful
                int lastBitPackedNum = byteCache.read();
                if (bitPackedGroupCount > 0) {

                    currentCount = (bitPackedGroupCount - 1) * config.RLE_MIN_REPEATED_NUM + lastBitPackedNum;
                    bitPackingNum = currentCount;
                } else {
                    throw new TSFileDecodingException(String.format(
                            "tsfile-encoding IntRleDecoder: bitPackedGroupCount %d, smaller than 1", bitPackedGroupCount));
                }
                readBitPackingBuffer(bitPackedGroupCount, lastBitPackedNum);
                break;
            default:
                throw new TSFileDecodingException(
                        String.format("tsfile-encoding IntRleDecoder: unknown encoding mode %s", mode));
        }
    }

    /**
     * read length and bit width of current package before we decode number
     *
     * @param in InputStream
     * @throws IOException cannot read length and bit-width
     */
    protected void readLengthAndBitWidth(InputStream in) throws IOException {
        // long st = System.currentTimeMillis();
        length = ReadWriteStreamUtils.readUnsignedVarInt(in);
        byte[] tmp = new byte[length];
        in.read(tmp, 0, length);
        byteCache = new ByteArrayInputStream(tmp);
        isLengthAndBitWidthReaded = true;
        bitWidth = byteCache.read();
        initPacker();
        // long et = System.currentTimeMillis();
    }

    /**
     * Check whether there is number left for reading
     *
     * @param in decoded data saved in InputStream
     * @return true or false to indicate whether there is number left
     * @throws IOException cannot check next value
     */
    @Override
    public boolean hasNext(InputStream in) throws IOException {
        if (currentCount > 0 || in.available() > 0 || hasNextPackage()) {
            return true;
        }
        return false;
    }

    /**
     * Check whether there is another pattern left for reading
     *
     * @return true or false to indicate whether there is another pattern left
     */
    protected boolean hasNextPackage() {
        return currentCount > 0 || byteCache.available() > 0;
    }

    protected abstract void initPacker();

    /**
     * Read rle package and save them in buffer
     *
     * @throws IOException cannot read number
     */
    protected abstract void readNumberInRLE() throws IOException;

    /**
     * Read bit-packing package and save them in buffer
     *
     * @param bitPackedGroupCount number of group number
     * @param lastBitPackedNum    number of useful value in last group
     * @throws IOException cannot read bit pack
     */
    protected abstract void readBitPackingBuffer(int bitPackedGroupCount, int lastBitPackedNum) throws IOException;

    @Override
    public boolean readBoolean(InputStream in) {
        throw new TSFileDecodingException("Method readBoolean is not supproted by RleDecoder");
    }

    @Override
    public short readShort(InputStream in) {
        throw new TSFileDecodingException("Method readShort is not supproted by RleDecoder");
    }

    @Override
    public int readInt(InputStream in) {
        throw new TSFileDecodingException("Method readInt is not supproted by RleDecoder");
    }

    @Override
    public long readLong(InputStream in) {
        throw new TSFileDecodingException("Method readLong is not supproted by RleDecoder");
    }

    @Override
    public float readFloat(InputStream in) {
        throw new TSFileDecodingException("Method readFloat is not supproted by RleDecoder");
    }

    @Override
    public double readDouble(InputStream in) {
        throw new TSFileDecodingException("Method readDouble is not supproted by RleDecoder");
    }

    @Override
    public Binary readBinary(InputStream in) {
        throw new TSFileDecodingException("Method readBinary is not supproted by RleDecoder");
    }

    @Override
    public BigDecimal readBigDecimal(InputStream in) {
        throw new TSFileDecodingException("Method readBigDecimal is not supproted by RleDecoder");
    }

    protected static enum MODE {
        RLE, BIT_PACKED
    }
}
