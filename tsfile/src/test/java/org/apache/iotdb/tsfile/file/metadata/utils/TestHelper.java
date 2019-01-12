package org.apache.iotdb.tsfile.file.metadata.utils;

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.file.metadata.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.file.metadata.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHelper {
    private static final String MAX_VALUE = "321";
    private static final String MIN_VALUE = "123";
    private static final String SUM_VALUE = "321123";
    private static final String FIRST_VALUE = "1";
    private static final String LAST_VALUE = "222";

    public static TsFileMetaData createSimpleFileMetaData() {
        TsFileMetaData metaData = new TsFileMetaData(generateDeviceIndexMetadataMap(), new HashMap<>(), TsFileMetaDataTest.VERSION);
        metaData.addMeasurementSchema(TestHelper.createSimpleMeasurementSchema());
        metaData.addMeasurementSchema(TestHelper.createSimpleMeasurementSchema());
        metaData.setCreatedBy(TsFileMetaDataTest.CREATED_BY);
        return metaData;
    }

    public static Map<String, TsDeviceMetadataIndex> generateDeviceIndexMetadataMap() {
        Map<String, TsDeviceMetadataIndex> indexMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            indexMap.put("device_" + i, createSimpleDeviceIndexMetadata());
        }
        return indexMap;
    }

    public static Map<String, TsDeviceMetadata> generateDeviceMetadataMap() {
        Map<String, TsDeviceMetadata> deviceMetadataMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            deviceMetadataMap.put("device_" + i, createSimpleDeviceMetaData());
        }
        return deviceMetadataMap;
    }

    public static TsDeviceMetadataIndex createSimpleDeviceIndexMetadata() {
        TsDeviceMetadataIndex index = new TsDeviceMetadataIndex();
        index.setOffset(0);
        index.setLen(10);
        index.setStartTime(100);
        index.setEndTime(200);
        return index;
    }

    public static TsDeviceMetadata createSimpleDeviceMetaData() {
        TsDeviceMetadata metaData = new TsDeviceMetadata();
        metaData.setStartTime(TsDeviceMetadataTest.START_TIME);
        metaData.setEndTime(TsDeviceMetadataTest.END_TIME);
        metaData.addChunkGroupMetaData(TestHelper.createSimpleChunkGroupMetaData());
        metaData.addChunkGroupMetaData(TestHelper.createSimpleChunkGroupMetaData());
        return metaData;
    }

    public static ChunkGroupMetaData createEmptySeriesChunkGroupMetaData(){
        ChunkGroupMetaData metaData = new ChunkGroupMetaData("d1",new ArrayList<>());
        return metaData;
    }

    public static ChunkGroupMetaData createSimpleChunkGroupMetaData() {
        ChunkGroupMetaData metaData = new ChunkGroupMetaData(ChunkGroupMetaDataTest.DELTA_OBJECT_UID, new ArrayList<>());
        metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaData());
        metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaData());
        return metaData;
    }

    public static ChunkMetaData createSimpleTimeSeriesChunkMetaData() {
        ChunkMetaData metaData =
                new ChunkMetaData(ChunkMetaDataTest.MEASUREMENT_UID, ChunkMetaDataTest.DATA_TYPE, ChunkMetaDataTest.FILE_OFFSET,
                        ChunkMetaDataTest.START_TIME, ChunkMetaDataTest.END_TIME//, ChunkMetaDataTest.ENCODING_TYPE
                );
        metaData.setNumOfPoints(ChunkMetaDataTest.NUM_OF_POINTS);
        metaData.setDigest(new TsDigest());
        return metaData;
    }

    public static MeasurementSchema createSimpleMeasurementSchema() {
        MeasurementSchema timeSeries = new MeasurementSchema(TimeSeriesMetadataTest.measurementUID, TSDataType.INT64, TSEncoding.RLE);
        return timeSeries;
    }

    public static TsDigest createSimpleTsDigest() {
        TsDigest digest = new TsDigest();
        digest.addStatistics("max", ByteBuffer.wrap(BytesUtils.StringToBytes(MAX_VALUE)));
        digest.addStatistics("min", ByteBuffer.wrap(BytesUtils.StringToBytes(MIN_VALUE)));
        digest.addStatistics("sum", ByteBuffer.wrap(BytesUtils.StringToBytes(SUM_VALUE)));
        digest.addStatistics("first", ByteBuffer.wrap(BytesUtils.StringToBytes(FIRST_VALUE)));
        digest.addStatistics("last", ByteBuffer.wrap(BytesUtils.StringToBytes(LAST_VALUE)));
        return digest;
    }

    public static List<String> getJSONArray() {
        List<String> jsonMetaData = new ArrayList<String>();
        jsonMetaData.add("fsdfsfsd");
        jsonMetaData.add("424fd");
        return jsonMetaData;
    }
}
