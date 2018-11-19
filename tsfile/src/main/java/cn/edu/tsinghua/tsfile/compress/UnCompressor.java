package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.common.exception.CompressionTypeNotSupportedException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * uncompress data according to type in metadata
 */
public abstract class UnCompressor {
    public static UnCompressor getUnCompressor(CompressionTypeName name) {
        if (name == null) {
            throw new CompressionTypeNotSupportedException("NULL");
        }
        switch (name) {
            case UNCOMPRESSED:
                return new NoUnCompressor();
            case SNAPPY:
                return new SnappyUnCompressor();
            default:
                throw new CompressionTypeNotSupportedException(name.toString());
        }
    }

    public abstract byte[] uncompress(byte[] byteArray);

    public abstract CompressionTypeName getCodecName();

    static public class NoUnCompressor extends UnCompressor {

        @Override
        public byte[] uncompress(byte[] byteArray) {
            return byteArray;
        }

        @Override
        public CompressionTypeName getCodecName() {
            return CompressionTypeName.UNCOMPRESSED;
        }
    }

    static public class SnappyUnCompressor extends UnCompressor {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnappyUnCompressor.class);

        @Override
        public byte[] uncompress(byte[] bytes) {
            if (bytes == null) {
                return null;
            }

            try {
                return Snappy.uncompress(bytes);
            } catch (IOException e) {
                LOGGER.error(
                        "tsfile-compression SnappyUnCompressor: errors occurs when uncompress input byte, bytes is {}",
                        bytes, e);
            }
            return null;
        }

        @Override
        public CompressionTypeName getCodecName() {
            return CompressionTypeName.SNAPPY;
        }
    }
}
