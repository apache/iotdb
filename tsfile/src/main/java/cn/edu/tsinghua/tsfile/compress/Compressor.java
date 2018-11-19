package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.common.exception.CompressionTypeNotSupportedException;
import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.common.utils.PublicBAOS;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * compress data according to type in schema
 */
public abstract class Compressor {
    public static Compressor getCompressor(String name) {
        return getCompressor(CompressionTypeName.valueOf(name));
    }

    public static Compressor getCompressor(CompressionTypeName name) {
        if (name == null) {
            throw new CompressionTypeNotSupportedException("NULL");
        }
        switch (name) {
            case UNCOMPRESSED:
                return new NoCompressor();
            case SNAPPY:
                return new SnappyCompressor();
            default:
                throw new CompressionTypeNotSupportedException(name.toString());
        }
    }

    public abstract ListByteArrayOutputStream compress(ListByteArrayOutputStream ListByteArray);

    public abstract CompressionTypeName getCodecName();

    /**
     * NoCompressor will do nothing for data and return the input data directly.
     *
     * @author kangrong
     */
    static public class NoCompressor extends Compressor {

        @Override
        public ListByteArrayOutputStream compress(ListByteArrayOutputStream ListByteArray) {
            return ListByteArray;
        }

        @Override
        public CompressionTypeName getCodecName() {
            return CompressionTypeName.UNCOMPRESSED;
        }
    }

    static public class SnappyCompressor extends Compressor {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnappyCompressor.class);

        @Override
        public ListByteArrayOutputStream compress(ListByteArrayOutputStream listByteArray) {
            if (listByteArray == null) {
                return null;
            }
            PublicBAOS out = new PublicBAOS();
            try {
                out.write(Snappy.compress(listByteArray.toByteArray()));
            } catch (IOException e) {
                LOGGER.error(
                        "tsfile-compression SnappyCompressor: errors occurs when compress input byte, ListByteArray is {}, ByteArrayOutputStream is {}",
                        listByteArray, out, e);
            }
            return ListByteArrayOutputStream.from(out);
        }

        @Override
        public CompressionTypeName getCodecName() {
            return CompressionTypeName.SNAPPY;
        }
    }
}
