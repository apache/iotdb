package cn.edu.tsinghua.tsfile.encoding.decoder;

import cn.edu.tsinghua.tsfile.common.exception.TSFileDecodingException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

/**
 * @author Zhang Jinrui
 */
public class PlainDecoder extends Decoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlainDecoder.class);
    public EndianType endianType;

    public PlainDecoder(EndianType endianType) {
        super(TSEncoding.PLAIN);
        this.endianType = endianType;
    }

    @Override
    public boolean readBoolean(InputStream in) {
        try {
            int ch1 = in.read();
            if (ch1 == 0) {
                return false;
            } else {
                return true;
            }
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding PlainDecoder: errors whewn read boolean", e);
        }
        return false;
    }

    @Override
    public short readShort(InputStream in) {
        try {
            int ch1 = in.read();
            int ch2 = in.read();
            if (this.endianType == EndianType.LITTLE_ENDIAN) {
                return (short) ((ch2 << 8) + ch1);
            } else {
                LOGGER.error(
                        "tsfile-encoding PlainEncoder: current version does not support short value decoding");
            }
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding PlainDecoder: errors whewn read short", e);
        }
        return -1;
    }

    @Override
    public int readInt(InputStream in) {
        try {
            int ch1 = in.read();
            int ch2 = in.read();
            int ch3 = in.read();
            int ch4 = in.read();
            if (this.endianType == EndianType.LITTLE_ENDIAN) {
                return ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
            } else {
                LOGGER.error(
                        "tsfile-encoding PlainEncoder: current version does not support int value encoding");
            }
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding PlainDecoder: errors whewn read int", e);
        }
        return -1;
    }

    @Override
    public long readLong(InputStream in) {
        int[] buf = new int[8];
        try {
            for (int i = 0; i < 8; i++)
                buf[i] = in.read();
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding PlainDecoder: errors whewn read long", e);
        }

        Long res = 0L;
        for (int i = 0; i < 8; i++) {
            res += ((long) buf[i] << (i * 8));
        }
        return res;
    }

    @Override
    public float readFloat(InputStream in) {
        return Float.intBitsToFloat(readInt(in));
    }

    @Override
    public double readDouble(InputStream in) {
        return Double.longBitsToDouble(readLong(in));
    }

    @Override
    public Binary readBinary(InputStream in) {
        int length = readInt(in);
        byte[] buf = new byte[length];
        try {
            in.read(buf, 0, buf.length);
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding PlainDecoder: errors whewn read binary", e);
        }
        return new Binary(buf);
    }

    @Override
    public boolean hasNext(InputStream in) throws IOException {
        return in.available() > 0;
    }

    @Override
    public BigDecimal readBigDecimal(InputStream in) {
        throw new TSFileDecodingException("Method readBigDecimal is not supproted by PlainDecoder");
    }
}
