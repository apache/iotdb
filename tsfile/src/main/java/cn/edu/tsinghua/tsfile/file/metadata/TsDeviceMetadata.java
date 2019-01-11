package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TsDeviceMetadata{

    /**
     * size of ChunkGroupMetadataBlock in byte
     **/
    private int serializedSize = 2 * Long.BYTES + Integer.BYTES;// this field does not need to be serialized.

    /**
     * start time for a device
     **/
    private long startTime = Long.MAX_VALUE;

    /**
     * end time for a device
     **/
    private long endTime = Long.MIN_VALUE;

    /**
     * Row groups in this file
     */
    private List<ChunkGroupMetaData> chunkGroupMetadataList = new ArrayList<>();


    public int getSerializedSize() {
        return serializedSize;
    }

    private void reCalculateSerializedSize() {
        serializedSize = 2 * Long.BYTES + // startTime , endTime
                Integer.BYTES; // size of chunkGroupMetadataList

        for (ChunkGroupMetaData meta : chunkGroupMetadataList) {
            serializedSize += meta.getSerializedSize();
        }
    }

    public TsDeviceMetadata() {
    }

    /**
     * set the ChunkGroupMetadataList and recalculate serialized size.
     * @param chunkGroupMetadataList
     */
    public void setChunkGroupMetadataList(List<ChunkGroupMetaData> chunkGroupMetadataList){
        this.chunkGroupMetadataList = chunkGroupMetadataList;
        reCalculateSerializedSize();
    }

    /**
     * add chunk group metadata to chunkGroups. THREAD NOT SAFE
     *
     * @param chunkGroup - chunk group metadata to add
     */
    public void addChunkGroupMetaData(ChunkGroupMetaData chunkGroup) {
        chunkGroupMetadataList.add(chunkGroup);
        serializedSize += chunkGroup.getSerializedSize();
        for (ChunkMetaData chunkMetaData : chunkGroup.getChunkMetaDataList()) {
            // update startTime and endTime
            startTime = Long.min(startTime, chunkMetaData.getStartTime());
            endTime = Long.max(endTime, chunkMetaData.getEndTime());
        }
    }

    public List<ChunkGroupMetaData> getChunkGroups() {
        return Collections.unmodifiableList(chunkGroupMetadataList);
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(startTime, outputStream);
        byteLen += ReadWriteIOUtils.write(endTime, outputStream);

        if (chunkGroupMetadataList == null) {
            byteLen += ReadWriteIOUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteIOUtils.write(chunkGroupMetadataList.size(), outputStream);
            for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetadataList)
                byteLen += chunkGroupMetaData.serializeTo(outputStream);
        }
        assert getSerializedSize() == byteLen;
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(startTime, buffer);
        byteLen += ReadWriteIOUtils.write(endTime, buffer);

        if (chunkGroupMetadataList == null) {
            byteLen += ReadWriteIOUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteIOUtils.write(chunkGroupMetadataList.size(), buffer);
            for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetadataList)
                byteLen += chunkGroupMetaData.serializeTo(buffer);
        }

        return byteLen;
    }

    public static TsDeviceMetadata deserializeFrom(InputStream inputStream) throws IOException {
        TsDeviceMetadata deviceMetadata = new TsDeviceMetadata();

        deviceMetadata.startTime = ReadWriteIOUtils.readLong(inputStream);
        deviceMetadata.endTime = ReadWriteIOUtils.readLong(inputStream);

        int size = ReadWriteIOUtils.readInt(inputStream);
        if (size > 0) {
            List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                chunkGroupMetaDataList.add(ChunkGroupMetaData.deserializeFrom(inputStream));
            }
            deviceMetadata.chunkGroupMetadataList = chunkGroupMetaDataList;
        }

        deviceMetadata.reCalculateSerializedSize();
        return deviceMetadata;
    }

    public static TsDeviceMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
        TsDeviceMetadata deviceMetadata = new TsDeviceMetadata();

        deviceMetadata.startTime = ReadWriteIOUtils.readLong(buffer);
        deviceMetadata.endTime = ReadWriteIOUtils.readLong(buffer);

        int size = ReadWriteIOUtils.readInt(buffer);
        if (size > 0) {
            List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                chunkGroupMetaDataList.add(ChunkGroupMetaData.deserializeFrom(buffer));
            }
            deviceMetadata.chunkGroupMetadataList = chunkGroupMetaDataList;
        }

        deviceMetadata.reCalculateSerializedSize();
        return deviceMetadata;
    }

    @Override
    public String toString() {
        return "TsDeviceMetadata{" +
                "serializedSize=" + serializedSize +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", chunkGroupMetadataList=" + chunkGroupMetadataList +
                '}';
    }

}
