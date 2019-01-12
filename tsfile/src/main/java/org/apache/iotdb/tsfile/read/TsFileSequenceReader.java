package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.read.reader.DefaultTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.compress.UnCompressor;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.compress.UnCompressor;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.DefaultTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TsFileSequenceReader {

    private TsFileInput tsFileInput;
    private long fileMetadataPos;
    private int fileMetadataSize;
    private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
    private String file;

    /**
     * Create a file reader of the given file.
     * The reader will read the tail of the file to get the file metadata size.
     * Then the reader will skip the first TSFileConfig.MAGIC_STRING.length() bytes of the file
     * for preparing reading real data.
     *
     * @param file the data file
     * @throws IOException If some I/O error occurs
     */
    public TsFileSequenceReader(String file) throws IOException {
        this(file, true);
    }


    public TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
        this.file = file;
        tsFileInput = new DefaultTsFileInput(Paths.get(file));
        if (loadMetadataSize) {
            loadMetadataSize();
        }
    }

    private void loadMetadataSize() throws IOException {
        ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
        tsFileInput.read(metadataSize, tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES);
        metadataSize.flip();
        //read file metadata size and position
        fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
        fileMetadataPos = tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES - fileMetadataSize;
        //skip the magic header
        tsFileInput.position(TSFileConfig.MAGIC_STRING.length());
    }


    /**
     * @param input            the input of a tsfile. The current position should be a markder and then a chunk Header,
     *                         rather than the magic number
     * @param fileMetadataPos  the position of the file metadata in the TsFileInput
     *                         from the beginning of the input to the current position
     * @param fileMetadataSize the byte size of the file metadata in the input
     */
    public TsFileSequenceReader(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
        this.tsFileInput = input;
        this.fileMetadataPos = fileMetadataPos;
        this.fileMetadataSize = fileMetadataSize;
    }

    public long getFileMetadataPos() {
        return fileMetadataPos;
    }

    public int getFileMetadataSize() {
        return fileMetadataSize;
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public String readTailMagic() throws IOException {
        long totalSize = tsFileInput.size();
        ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.length());
        magicStringBytes.flip();
        return new String(magicStringBytes.array());
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public String readHeadMagic() throws IOException {
        ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        tsFileInput.read(magicStringBytes, 0);
        magicStringBytes.flip();
        return new String(magicStringBytes.array());
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public TsFileMetaData readFileMetadata() throws IOException {
        return TsFileMetaData.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public TsDeviceMetadata readTsDeviceMetaData(TsDeviceMetadataIndex index) throws IOException {
        return TsDeviceMetadata.deserializeFrom(readData(index.getOffset(), index.getLen()));
    }

    /**
     * read data from current position of the input, and deserialize it to a ChunkGroupFooter.
     * <br>This method is not threadsafe.</>
     * @return a ChunkGroupFooter
     * @throws IOException io error
     */
    public ChunkGroupFooter readChunkGroupFooter() throws IOException {
        return ChunkGroupFooter.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
    }
    /**
     * read data from current position of the input, and deserialize it to a ChunkGroupFooter.
     * @param position the offset of the chunk group footer in the file
     * @param markerRead  true if the offset does not contains the marker , otherwise false
     * @return a ChunkGroupFooter
     * @throws IOException io error
     */
    public ChunkGroupFooter readChunkGroupFooter(long position, boolean markerRead) throws IOException {
        return ChunkGroupFooter.deserializeFrom(tsFileInput.wrapAsFileChannel(), position, markerRead);
    }

    /**
     * After reading the footer of a ChunkGroup, call this method to set the file pointer to the start of the data of this
     * ChunkGroup if you want to read its data next.
     * <br>This method is not threadsafe.</>
     * @param footer the chunkGroupFooter which you want to read data
     */
    public void setPositionToAChunkGroup(ChunkGroupFooter footer) throws IOException {
        tsFileInput.position(tsFileInput.position() - footer.getDataSize() - footer.getSerializedSize());
    }


    /**
     * read data from current position of the input, and deserialize it to a ChunkHeader.
     * <br>This method is not threadsafe.</>
     * @return a ChunkHeader
     * @throws IOException io error
     */
    public ChunkHeader readChunkHeader() throws IOException {
        return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
    }

    /**
     * notice, the position of the channel MUST be at the end of this header.
     * <br>This method is not threadsafe.</>
     * @return the pages of this chunk
     */
    public ByteBuffer readChunk(ChunkHeader header) throws IOException {
        return readData(-1, header.getDataSize());
    }

    /**
     * notice, this function will modify channel's position.
     * @param  position the offset of the chunk data
     * @return the pages of this chunk
     */
    public ByteBuffer readChunk(ChunkHeader header, long position) throws IOException {
        return readData(position, header.getDataSize());
    }

    public Chunk readMemChunk(ChunkMetaData metaData) throws IOException {
        ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), false);
        ByteBuffer buffer = readChunk(metaData.getOffsetOfChunkHeader() + header.getSerializedSize(), header.getDataSize());
        return new Chunk(header, buffer);
    }

    /**
     * @param position the file offset of this chunk's header
     * @param markerRead true if the offset does not contains the marker , otherwise false
     */
    private ChunkHeader readChunkHeader(long position, boolean markerRead) throws IOException {
        return ChunkHeader.deserializeFrom(tsFileInput.wrapAsFileChannel(), position, markerRead);
    }

    /**
     * notice, this function will modify channel's position.
     * @param dataSize the size of chunkdata
     * @param  position the offset of the chunk data
     * @return the pages of this chunk
     */
    private ByteBuffer readChunk(long position, int dataSize) throws IOException {
        return readData(position, dataSize);
    }

    /**
     * not thread safe.
     * @param type
     * @return
     * @throws IOException
     */
    public PageHeader readPageHeader(TSDataType type) throws IOException {
        return readPageHeader(type, -1);
    }

    /**
     * notice, this function will modify channel's position.
     *
     * @param position the file offset of this page header's header
     */
    private PageHeader readPageHeader(TSDataType type, long position) throws IOException {
        int size = PageHeader.calculatePageHeaderSize(type);
        ByteBuffer buffer = readData(position, size);
        return PageHeader.deserializeFrom(buffer, type);
    }

    public long position() throws IOException {
        return tsFileInput.position();
    }

    public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
        return readPage(header, type, -1);
    }

    private ByteBuffer readPage(PageHeader header, CompressionType type, long position) throws IOException {
        ByteBuffer buffer = readData(position, header.getCompressedSize());
        UnCompressor unCompressor = UnCompressor.getUnCompressor(type);
        ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
        //unCompressor.uncompress(buffer, uncompressedBuffer);
        //uncompressedBuffer.flip();
        switch (type) {
            case UNCOMPRESSED:
                return buffer;
            default:
                unCompressor.uncompress(buffer.array(), buffer.position(), buffer.remaining(), uncompressedBuffer.array(), 0);//FIXME if the buffer is not array-impelmented.
                return uncompressedBuffer;
        }
    }


    /**
     * read one byte from the input.
     * <br> this method is not thread safe</>
     */
    public byte readMarker() throws IOException {
        markerBuffer.clear();
        ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), markerBuffer);
        markerBuffer.flip();
        return markerBuffer.get();
    }

    public byte readMarker(long position) throws IOException {
        return readData(position, Byte.BYTES).get();
    }

    public void close() throws IOException {
        this.tsFileInput.close();
    }

    public String getFileName() {
        return this.file;
    }

    /**
     * read data from tsFileInput, from the current position (if position = -1), or the given position.
     * <br> if position = -1, the tsFileInput's position will be changed to the current position + real data size that been read.
     * Other wise, the tsFileInput's position is not changed.
     * @param position the start position of data in the tsFileInput, or the current position if position = -1
     * @param size the size of data that want to read
     * @return data that been read.
     */
    private ByteBuffer readData(long position, int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        if (position == -1) {
            ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), buffer);
        } else {
            ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), buffer, position, size);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * notice, the target bytebuffer are not flipped.
     */
    public int readRaw(long position, int length, ByteBuffer target) throws IOException {
        return ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), target, position, length);
    }
}
