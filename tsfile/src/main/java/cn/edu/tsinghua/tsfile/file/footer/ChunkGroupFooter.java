package cn.edu.tsinghua.tsfile.file.footer;

import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.MetaMarker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ChunkGroupFooter {

    private static final byte MARKER = MetaMarker.ChunkGroupFooter;

    private String deviceID;

    private long dataSize;

    private int numberOfChunks;

    /**
     * The time when endRowgroup() is called.
     */
    private long writtenTime;

    //this field does not need to be serialized.
    private int serializedSize;

    public int getSerializedSize() {
        return serializedSize;
    }


    public ChunkGroupFooter(String deviceID, long dataSize, int numberOfChunks) {
        this.deviceID = deviceID;
        this.dataSize = dataSize;
        this.numberOfChunks = numberOfChunks;
        this.serializedSize = Byte.BYTES + Integer.BYTES + deviceID.length() + Long.BYTES + Integer.BYTES;
    }

    public String getDeviceID() {
        return deviceID;
    }

    public long getDataSize() {
        return dataSize;
    }

    public int getNumberOfChunks() {
        return numberOfChunks;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int length = 0;
        length += ReadWriteIOUtils.write(MARKER, outputStream);
        length += ReadWriteIOUtils.write(deviceID, outputStream);
        length += ReadWriteIOUtils.write(dataSize, outputStream);
        length += ReadWriteIOUtils.write(numberOfChunks, outputStream);
        assert length == getSerializedSize();
        return length;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    /**
     * @param inputStream
     * @param markerRead  Whether the marker of the ChunkGroupFooter is read ahead.
     * @return
     * @throws IOException
     */
    public static ChunkGroupFooter deserializeFrom(InputStream inputStream, boolean markerRead) throws IOException {
        if (!markerRead) {
            byte marker = (byte) inputStream.read();
            if (marker != MARKER)
                MetaMarker.handleUnexpectedMarker(marker);
        }

        String deviceID = ReadWriteIOUtils.readString(inputStream);
        long dataSize = ReadWriteIOUtils.readLong(inputStream);
        int numOfChunks = ReadWriteIOUtils.readInt(inputStream);
        return new ChunkGroupFooter(deviceID, dataSize, numOfChunks);
    }


    /**
     * @param channel
     * @param markerRead Whether the marker of the ChunkGroupFooter is read ahead.
     * @return
     * @throws IOException
     */
    public static ChunkGroupFooter deserializeFrom(FileChannel channel, long offset, boolean markerRead) throws IOException {
        if (!markerRead) {
            offset++;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, offset);
        buffer.flip();
        int size = buffer.getInt();
        offset += Integer.BYTES;
        buffer = ByteBuffer.allocate(getSerializedSize(size));
        ReadWriteIOUtils.readAsPossible(channel, offset, buffer);
        buffer.flip();
        String deviceID = ReadWriteIOUtils.readStringWithoutLength(buffer, size);
        long dataSize = ReadWriteIOUtils.readLong(buffer);
        int numOfChunks = ReadWriteIOUtils.readInt(buffer);
        return new ChunkGroupFooter(deviceID, dataSize, numOfChunks);
    }


    public static int getSerializedSize(String deviceID) {
        return Byte.BYTES + Integer.BYTES + getSerializedSize(deviceID.length());
    }

    private static int getSerializedSize(int deviceIDLength) {
        return deviceIDLength + Long.BYTES + Integer.BYTES;
    }

    @Override
    public String toString() {
        return "ChunkGroupFooter{" +
                "deviceID='" + deviceID + '\'' +
                ", dataSize=" + dataSize +
                ", numberOfChunks=" + numberOfChunks +
                ", serializedSize=" + serializedSize +
                '}';
    }
}
