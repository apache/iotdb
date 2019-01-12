package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TsDeviceMetadataIndex {
    /**
     * The offset of the TsDeviceMetadata
     */
    private long offset;
    /**
     * The size of the TsDeviceMetadata in the disk
     */
    private int len;
    /**
     * The start time of the device
     */
    private long startTime;
    /**
     * The end time of the device
     */
    private long endTime;

    public TsDeviceMetadataIndex(){

    }

    public TsDeviceMetadataIndex(long offset, int len, TsDeviceMetadata deviceMetadata) {
        this.offset = offset;
        this.len = len;
        this.startTime = deviceMetadata.getStartTime();
        this.endTime = deviceMetadata.getEndTime();
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getOffset() {
        return offset;
    }

    public int getLen() {
        return len;
    }

    public long getStartTime() {
        return startTime;
    }


    public long getEndTime() {
        return endTime;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(offset, outputStream);
        byteLen += ReadWriteIOUtils.write(len, outputStream);
        byteLen += ReadWriteIOUtils.write(startTime, outputStream);
        byteLen += ReadWriteIOUtils.write(endTime, outputStream);
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(offset, buffer);
        byteLen += ReadWriteIOUtils.write(len, buffer);
        byteLen += ReadWriteIOUtils.write(startTime, buffer);
        byteLen += ReadWriteIOUtils.write(endTime, buffer);
        return byteLen;
    }

    public static TsDeviceMetadataIndex deserializeFrom(InputStream inputStream) throws IOException {
        TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
        index.offset = ReadWriteIOUtils.readLong(inputStream);
        index.len = ReadWriteIOUtils.readInt(inputStream);
        index.startTime = ReadWriteIOUtils.readLong(inputStream);
        index.endTime = ReadWriteIOUtils.readLong(inputStream);
        return index;
    }

    public static TsDeviceMetadataIndex deserializeFrom(ByteBuffer buffer) throws IOException {
        TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
        index.offset = ReadWriteIOUtils.readLong(buffer);
        index.len = ReadWriteIOUtils.readInt(buffer);
        index.startTime = ReadWriteIOUtils.readLong(buffer);
        index.endTime = ReadWriteIOUtils.readLong(buffer);
        return index;
    }

    @Override
    public String toString() {
        return "TsDeviceMetadataIndex{" +
                "offset=" + offset +
                ", len=" + len +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
