package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Metadata of ChunkGroup
 */
public class ChunkGroupMetaData {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkGroupMetaData.class);

    /**
     * Name of device, this field is not serialized.
     */
    private String deviceID;

    /**
     * Byte size of this metadata. this field is not serialized.
     */
    private int serializedSize;

    /**
     * All time series chunks in this chunk group.
     */
    private List<ChunkMetaData> chunkMetaDataList;

    public int getSerializedSize() {
        return serializedSize;
    }


    private ChunkGroupMetaData() {
        chunkMetaDataList = new ArrayList<>();
    }

    /**
     * @param deviceID               name of device
     * @param chunkMetaDataList all time series chunks in this chunk group. Can not be Null.
     *                                    notice: after constructing a ChunkGroupMetadata instance. Donot use list.add()
     *                                    to modify `chunkMetaDataList`. Instead, use addTimeSeriesChunkMetaData()
     *                                    to make sure  getSerializedSize() is correct.
     */
    public ChunkGroupMetaData(String deviceID, List<ChunkMetaData> chunkMetaDataList) {
        assert chunkMetaDataList != null;
        this.deviceID = deviceID;
        this.chunkMetaDataList = chunkMetaDataList;
        reCalculateSerializedSize();
    }

    void reCalculateSerializedSize(){
        serializedSize = Integer.BYTES + deviceID.length() +
                Integer.BYTES; // size of chunkMetaDataList
        for (ChunkMetaData chunk : chunkMetaDataList) {
            serializedSize += chunk.getSerializedSize();
        }
    }
    /**
     * add time series chunk metadata to list. THREAD NOT SAFE
     *
     * @param metadata time series metadata to add
     */
    public void addTimeSeriesChunkMetaData(ChunkMetaData metadata) {
        if (chunkMetaDataList == null) {
            chunkMetaDataList = new ArrayList<>();
        }
        chunkMetaDataList.add(metadata);
        serializedSize += metadata.getSerializedSize();
    }

    public List<ChunkMetaData> getChunkMetaDataList() {
        return chunkMetaDataList == null ? null
                : Collections.unmodifiableList(chunkMetaDataList);
    }


    @Override
    public String toString() {
        return String.format(
                "ChunkGroupMetaData{ time series chunk list: %s }", chunkMetaDataList);
    }


    public String getDeviceID() {
        return deviceID;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(deviceID, outputStream);

        byteLen += ReadWriteIOUtils.write(chunkMetaDataList.size(), outputStream);
        for (ChunkMetaData chunkMetaData : chunkMetaDataList)
            byteLen += chunkMetaData.serializeTo(outputStream);
        assert byteLen == getSerializedSize();
        return byteLen;
    }


    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(deviceID, buffer);

        byteLen += ReadWriteIOUtils.write(chunkMetaDataList.size(), buffer);
        for (ChunkMetaData chunkMetaData : chunkMetaDataList)
            byteLen += chunkMetaData.serializeTo(buffer);
        assert byteLen == getSerializedSize();

        return byteLen;
    }

    public static ChunkGroupMetaData deserializeFrom(InputStream inputStream) throws IOException {
        ChunkGroupMetaData chunkGroupMetaData = new ChunkGroupMetaData();

        chunkGroupMetaData.deviceID = ReadWriteIOUtils.readString(inputStream);

        int size = ReadWriteIOUtils.readInt(inputStream);
        chunkGroupMetaData.serializedSize = Integer.BYTES + chunkGroupMetaData.deviceID.length() + Integer.BYTES;

        List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            ChunkMetaData metaData = ChunkMetaData.deserializeFrom(inputStream);
            chunkMetaDataList.add(metaData);
            chunkGroupMetaData.serializedSize += metaData.getSerializedSize();
        }
        chunkGroupMetaData.chunkMetaDataList = chunkMetaDataList;


        return chunkGroupMetaData;
    }

    public static ChunkGroupMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
        ChunkGroupMetaData chunkGroupMetaData = new ChunkGroupMetaData();

        chunkGroupMetaData.deviceID = (ReadWriteIOUtils.readString(buffer));

        int size = ReadWriteIOUtils.readInt(buffer);

        chunkGroupMetaData.serializedSize = Integer.BYTES + chunkGroupMetaData.deviceID.length() + Integer.BYTES;


        List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ChunkMetaData metaData = ChunkMetaData.deserializeFrom(buffer);
            chunkMetaDataList.add(metaData);
            chunkGroupMetaData.serializedSize += metaData.getSerializedSize();
        }
        chunkGroupMetaData.chunkMetaDataList = chunkMetaDataList;

        return chunkGroupMetaData;
    }


}
