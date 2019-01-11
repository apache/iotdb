package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.exception.compress.CompressionTypeNotSupportedException;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * compress data according to type in schema
 */
public abstract class Compressor {

    public static Compressor getCompressor(String name) {
        return getCompressor(CompressionType.valueOf(name));
    }

    /**
     * get Compressor according to CompressionType
     * @param name CompressionType
     * @return the Compressor of specified CompressionType
     */
    public static Compressor getCompressor(CompressionType name) {
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

    public abstract byte[] compress(byte[] data) throws IOException;

    /**
     *
     * @param data
     * @param offset
     * @param length
     * @param compressed
     * @return byte length of compressed data.
     * @throws IOException
     */
    public abstract  int  compress(byte[] data, int offset, int length, byte[] compressed) throws IOException;

    /**
     * If the data is large, this function is better than byte[].
     * @param data MUST be DirectByteBuffer  for Snappy.
     * @param compressed MUST be DirectByteBuffer for Snappy.
     * @return byte length of compressed data.
     * @throws IOException
     */
    public abstract int compress(ByteBuffer data, ByteBuffer compressed) throws IOException;

    public abstract  int getMaxBytesForCompression(int uncompressedDataSize);

    public abstract CompressionType getType();

    /**
     * NoCompressor will do nothing for data and return the input data directly.
     */
    static public class NoCompressor extends Compressor {

        @Override
        public byte[] compress(byte[] data) {
            return data;
        }

        @Override
        public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
            throw new IOException("No Compressor does not support compression function");
        }

        @Override
        public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
            throw new IOException("No Compressor does not support compression function");
        }

        @Override
        public int getMaxBytesForCompression(int uncompressedDataSize) {
            return uncompressedDataSize;
        }

        @Override
        public CompressionType getType() {
            return CompressionType.UNCOMPRESSED;
        }
    }

    static public class SnappyCompressor extends Compressor {
        private static final Logger LOGGER = LoggerFactory.getLogger(SnappyCompressor.class);

        @Override
        public byte[] compress(byte[] data) throws IOException {
            if (data == null) {
                return null;
            }
            return Snappy.compress(data);
        }

        @Override
        public int compress(byte[] data, int offset, int length, byte[] compressed) throws IOException {
            return Snappy.compress(data, offset, length, compressed, 0);
        }

        @Override
        public int compress(ByteBuffer data, ByteBuffer compressed) throws IOException {
             return Snappy.compress(data, compressed);
        }

        @Override
        public int getMaxBytesForCompression(int uncompressedDataSize) {
            return Snappy.maxCompressedLength(uncompressedDataSize);
        }

        @Override
        public CompressionType getType() {
            return CompressionType.SNAPPY;
        }
    }
}
