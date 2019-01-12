package org.apache.iotdb.tsfile.compress;

import org.apache.iotdb.tsfile.exception.compress.CompressionTypeNotSupportedException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * uncompress data according to type in metadata
 */
public abstract class UnCompressor {

    /**
     * get the UnCompressor based on the CompressionType
     * @param name CompressionType
     * @return the UnCompressor of specified CompressionType
     */
    public static UnCompressor getUnCompressor(CompressionType name) {
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

    public abstract int getUncompressedLength(byte[] array, int offset, int length) throws IOException;

    /**
     *
     * @param buffer MUST be DirectByteBuffer
     * @return
     * @throws IOException
     */
    public abstract int getUncompressedLength(ByteBuffer buffer) throws IOException;

    /**
     * uncompress the byte array
     * @param byteArray to be uncompressed bytes
     * @return bytes after uncompressed
     */
    public abstract byte[] uncompress(byte[] byteArray);

    /**
     *
     * @param byteArray
     * @param offset
     * @param length
     * @param output
     * @param outOffset
     * @return the valid length of the output array
     */
    public abstract  int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)  throws IOException ;

    /**
     * if the data is large, using this function is better.
     * @param compressed MUST be DirectByteBuffer
     * @param uncompressed MUST be DirectByteBuffer
     * @return
     */
    public abstract int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)   throws IOException ;

    public abstract CompressionType getCodecName();

    static public class NoUnCompressor extends UnCompressor {

        @Override
        public int getUncompressedLength(byte[] array, int offset, int length) {
            return length;
        }

        @Override
        public int getUncompressedLength(ByteBuffer buffer) {
            return buffer.remaining();
        }

        @Override
        public byte[] uncompress(byte[] byteArray) {
            return byteArray;
        }

        @Override
        public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset)  throws IOException {
            throw new IOException("NoUnCompressor does not support this method.");
        }

        @Override
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed)   throws IOException {
            throw new IOException("NoUnCompressor does not support this method.");
        }

        @Override
        public CompressionType getCodecName() {
            return CompressionType.UNCOMPRESSED;
        }
    }

    static public class SnappyUnCompressor extends UnCompressor {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnappyUnCompressor.class);

        @Override
        public int getUncompressedLength(byte[] array, int offset, int length) throws IOException {
            return Snappy.uncompressedLength(array, offset, length);
        }

        @Override
        public int getUncompressedLength(ByteBuffer buffer) throws IOException {
            return Snappy.uncompressedLength(buffer);
        }

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
        public int uncompress(byte[] byteArray, int offset, int length, byte[] output, int outOffset) throws IOException {
            Snappy.uncompressedLength(byteArray, offset, length);
            return Snappy.uncompress(byteArray, offset, length, output, outOffset);
        }


        @Override
        public int uncompress(ByteBuffer compressed, ByteBuffer uncompressed) {
            if (compressed == null || !compressed.hasRemaining()) {
                return 0;
            }

            try {
                return Snappy.uncompress(compressed, uncompressed);
            } catch (IOException e) {
                LOGGER.error(
                        "tsfile-compression SnappyUnCompressor: errors occurs when uncompress input byte, bytes is {}",
                        compressed.array(), e);
            }
            return 0;
        }

        @Override
        public CompressionType getCodecName() {
            return CompressionType.SNAPPY;
        }
    }
}
