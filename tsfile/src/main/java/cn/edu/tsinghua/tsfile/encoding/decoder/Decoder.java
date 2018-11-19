package cn.edu.tsinghua.tsfile.encoding.decoder;

import cn.edu.tsinghua.tsfile.common.exception.TSFileDecodingException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.format.Encoding;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;

/**
 * @author Zhang Jinrui
 */
public abstract class Decoder {
    public TSEncoding type;

    public Decoder(TSEncoding type) {
        this.type = type;
    }

    public static Decoder getDecoderByType(Encoding type, TSDataType dataType) {
        // PLA and DFT encoding are not supported in current version
        if (type == Encoding.PLAIN) {
            return new PlainDecoder(EndianType.LITTLE_ENDIAN);
        } else if (type == Encoding.RLE && dataType == TSDataType.BOOLEAN) {
        	return new IntRleDecoder(EndianType.LITTLE_ENDIAN);
		}  else if (type == Encoding.TS_2DIFF && dataType == TSDataType.INT32) {
            return new DeltaBinaryDecoder.IntDeltaDecoder();
        } else if (type == Encoding.TS_2DIFF && dataType == TSDataType.INT64) {
            return new DeltaBinaryDecoder.LongDeltaDecoder();
        } else if (type == Encoding.RLE && dataType == TSDataType.INT32) {
            return new IntRleDecoder(EndianType.LITTLE_ENDIAN);
        } else if (type == Encoding.RLE && dataType == TSDataType.INT64) {
            return new LongRleDecoder(EndianType.LITTLE_ENDIAN);
        } else if (type == Encoding.BITMAP && dataType == TSDataType.ENUMS) {
            return new BitmapDecoder(EndianType.LITTLE_ENDIAN);
        } else if ((dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE) && (type == Encoding.RLE || type == Encoding.TS_2DIFF) ) {
            return new FloatDecoder(TSEncoding.valueOf(type.toString()), dataType);
        } else if (type == Encoding.GORILLA && dataType == TSDataType.FLOAT) {
            return new SinglePrecisionDecoder();
        } else if (type == Encoding.GORILLA && dataType == TSDataType.DOUBLE) {
            return new DoublePrecisionDecoder();
        } else {
            throw new TSFileDecodingException("Decoder not found:" + type + " , DataType is :" + dataType);
        }
    }

    public int readInt(InputStream in) {
        throw new TSFileDecodingException("Method readInt is not supproted by Decoder");
    }

    public boolean readBoolean(InputStream in) {
        throw new TSFileDecodingException("Method readBoolean is not supproted by Decoder");
    }

    public short readShort(InputStream in) {
        throw new TSFileDecodingException("Method readShort is not supproted by Decoder");
    }

    public long readLong(InputStream in) {
        throw new TSFileDecodingException("Method readLong is not supproted by Decoder");
    }

    public float readFloat(InputStream in) {
        throw new TSFileDecodingException("Method readFloat is not supproted by Decoder");
    }

    public double readDouble(InputStream in) {
        throw new TSFileDecodingException("Method readDouble is not supproted by Decoder");
    }

    public Binary readBinary(InputStream in) {
        throw new TSFileDecodingException("Method readBinary is not supproted by Decoder");
    }

    public BigDecimal readBigDecimal(InputStream in) {
        throw new TSFileDecodingException("Method readBigDecimal is not supproted by Decoder");
    }

    public abstract boolean hasNext(InputStream in) throws IOException;
}
