package cn.edu.tsinghua.tsfile.encoding.encoder;

import cn.edu.tsinghua.tsfile.exception.encoding.TSFileEncodingException;
import cn.edu.tsinghua.tsfile.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * This class is the parent class of all Encoders. Every encoder has a specific
 * {@code <encoderType>} which represents the type of this encoder
 *
 * @author Zhang Jinrui
 */
public abstract class Encoder {
    public static final String MAX_STRING_LENGTH = "max_string_length";
    public static final String MAX_POINT_NUMBER = "max_point_number";

    public TSEncoding type;

    public Encoder(TSEncoding type) {
        this.type = type;
    }

    public void encode(boolean value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode boolean is not supported by Encoder");
    }

    public void encode(short value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode short is not supported by Encoder");
    }

    public void encode(int value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode int is not supported by Encoder");
    }

    public void encode(long value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode long is not supported by Encoder");
    }

    public void encode(float value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode float is not supported by Encoder");
    }

    public void encode(double value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode double is not supported by Encoder");
    }

    public void encode(Binary value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode Binary is not supported by Encoder");
    }

    public void encode(BigDecimal value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException("Method encode BigDecimal is not supported by Encoder");
    }

    /**
     * Write all values buffered in memory cache to OutputStream
     *
     * @param out - ByteArrayOutputStream
     * @throws IOException cannot flush to OutputStream
     */
    public abstract void flush(ByteArrayOutputStream out) throws IOException;

    /**
     * When encoder accepts a new incoming data point, the maximal possible size in byte it takes to store in memory.
     *
     * @return the maximal possible size of one data item encoded by this encoder
     */
    public int getOneItemMaxSize() {
        throw new UnsupportedOperationException();
    }

    /**
     * The maximal possible memory size occupied by current Encoder. This statistic value doesn't involve OutputStream.
     *
     * @return the maximal size of possible memory occupied by current encoder
     */
    public long getMaxByteSize() {
        throw new UnsupportedOperationException();
    }
}
