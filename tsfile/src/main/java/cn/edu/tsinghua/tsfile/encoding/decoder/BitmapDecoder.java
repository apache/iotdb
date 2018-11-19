package cn.edu.tsinghua.tsfile.encoding.decoder;

import cn.edu.tsinghua.tsfile.common.exception.TSFileDecodingException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decoder switch or enums value using bitmap, bitmap-encoding:
 * {@code <length> <num> <encoded-data>}
 */
public class BitmapDecoder extends Decoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(BitmapDecoder.class);

    /**
     * how many bytes for all encoded data in inputstream
     */
    private int length;

    /**
     * number of encoded data
     */
    private int number;

    /**
     * number of data left for reading in current buffer
     */
    private int currentCount;

    /**
     * each time decoder receives a inputstream, decoder creates a buffer to save all encoded data
     */
    private ByteArrayInputStream byteCache;

    /**
     * decoder reads all bitmap index from byteCache and save in Map<value, bitmap index>
     */
    private Map<Integer, byte[]> buffer;

    /**
     * @param endianType deprecated
     */
    public BitmapDecoder(EndianType endianType) {
        super(TSEncoding.BITMAP);
        byteCache = new ByteArrayInputStream(new byte[0]);
        buffer = new HashMap<>();
        length = 0;
        number = 0;
        currentCount = 0;
        LOGGER.debug("tsfile-encoding BitmapDecoder: init bitmap decoder");
    }

    @Override
    public int readInt(InputStream in) {
        if (currentCount == 0) {
            try {
                reset();
                getLengthAndNumber(in);
                readNext();
            } catch (IOException e) {
                LOGGER.error(
                        "tsfile-encoding BitmapDecoder: error occurs when reading next number. lenght {}, number {}, current number {}, result buffer {}",
                        length, number, currentCount, buffer, e);
            }
        }
        int result = 0;
        int index = (number - currentCount) / 8;
        int offset = 7 - ((number - currentCount) % 8);
        for (Map.Entry<Integer, byte[]> entry : buffer.entrySet()) {
            byte[] tmp = entry.getValue();
            if ((tmp[index] & ((byte) 1 << offset)) != 0) {
                result = entry.getKey();
            }
        }
        currentCount--;
        return result;
    }

    private void getLengthAndNumber(InputStream in) throws IOException {
        this.length = ReadWriteStreamUtils.readUnsignedVarInt(in);
        this.number = ReadWriteStreamUtils.readUnsignedVarInt(in);
        byte[] tmp = new byte[length];
        in.read(tmp, 0, length);
        this.byteCache = new ByteArrayInputStream(tmp);
    }

    /**
     * Decode all data from buffer and save them
     */
    private void readNext() throws IOException {
        int len = (this.number + 7) / 8;
        while (byteCache.available() > 0) {
            int value = ReadWriteStreamUtils.readUnsignedVarInt(byteCache);
            byte[] tmp = new byte[len];
            byteCache.read(tmp, 0, len);
            buffer.put(value, tmp);
        }
        currentCount = number;
    }

    private void reset() {
        this.length = 0;
        this.number = 0;
        this.currentCount = 0;
        if (this.byteCache == null) {
            new ByteArrayInputStream(new byte[0]);
        } else {
            this.byteCache.reset();
        }
        if (this.buffer == null) {
            this.buffer = new HashMap<>();
        } else {
            this.buffer.clear();
        }
    }

    /**
     * For special value in page list, get its bitmap index
     *
     * @param target   value to get its bitmap index
     * @param pageList input page list
     * @return List(Pair of (length, bitmap index) )
     */
    public List<Pair<Integer, byte[]>> decodeAll(int target, List<InputStream> pageList) {
        List<Pair<Integer, byte[]>> resultList = new ArrayList<>();
        for (InputStream inputStream : pageList) {
            try {
                reset();
                getLengthAndNumber(inputStream);
                int byteArrayLength = (this.number + 7) / 8;
                byte[] tmp = new byte[byteArrayLength];
                while (byteCache.available() > 0) {
                    int value = ReadWriteStreamUtils.readUnsignedVarInt(byteCache);
                    if (value == target) {
                        byteCache.read(tmp, 0, byteArrayLength);
                        break;
                    } else {
                        byteCache.skip(byteArrayLength);
                    }
                }

                resultList.add(new Pair<>(this.number, tmp));
                LOGGER.debug("tsfile-encoding BitmapDecoder: number {} in current page, byte length {}",
                        this.number, byteArrayLength);
            } catch (IOException e) {
                LOGGER.error(
                        "tsfile-encoding BitmapDecoder: error occurs when decoding all numbers in page {}, number {}",
                        inputStream, this.number, e);
            }
        }
        return resultList;
    }

    /**
     * Check whether there is number left for reading
     *
     * @param in : decoded data saved in InputStream
     * @return true or false to indicate whether there is number left
     * @throws IOException cannot read next value
     * @see Decoder#hasNext(java.io.InputStream)
     */
    @Override
    public boolean hasNext(InputStream in) throws IOException {
        if (currentCount > 0 || in.available() > 0) {
            return true;
        }
        return false;
    }

    /**
     * In current version, boolean value is equal to Enums value in schema
     *
     * @param in : decoded data saved in InputStream
     * @throws TSFileDecodingException cannot read next value
     * @see Decoder#readBoolean(java.io.InputStream)
     */
    @Override
    public boolean readBoolean(InputStream in) {
        throw new TSFileDecodingException("Method readBoolean is not supported by BitmapDecoder");
    }

    @Override
    public short readShort(InputStream in) {
        throw new TSFileDecodingException("Method readShort is not supported by BitmapDecoder");
    }

    @Override
    public long readLong(InputStream in) {
        throw new TSFileDecodingException("Method readLong is not supported by BitmapDecoder");
    }

    @Override
    public float readFloat(InputStream in) {
        throw new TSFileDecodingException("Method readFloat is not supported by BitmapDecoder");
    }

    @Override
    public double readDouble(InputStream in) {
        throw new TSFileDecodingException("Method readDouble is not supported by BitmapDecoder");
    }

    @Override
    public Binary readBinary(InputStream in) {
        throw new TSFileDecodingException("Method readBinary is not supported by BitmapDecoder");
    }

    @Override
    public BigDecimal readBigDecimal(InputStream in) {
        throw new TSFileDecodingException("Method readBigDecimal is not supported by BitmapDecoder");
    }
}
