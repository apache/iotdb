package cn.edu.tsinghua.iotdb.queryV2.debug;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowIO;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithoutFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dear Kuner, do you remember the night we find the big bug ?
 * Created by zhangjinrui on 2018/1/26.
 */
@Ignore
public class ForDebug {

    private static final int FOOTER_LENGTH = 4;
    private static final int POS_LENGTH = 8;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

    String tsfilePath = "/users/zhangjinrui/Desktop/readTest/tsfile";
    String unseqTsfilePath = "/users/zhangjinrui/Desktop/readTest/unseqTsfile";

    Path path = new Path("root.performf.group_23.d_2301.s_1");

    public void test() throws IOException {

        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(tsfilePath);
        TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);
        long startTime = tsFileMetaData.getDeltaObject(path.getDeltaObjectToString()).startTime;
        long endTime = tsFileMetaData.getDeltaObject(path.getDeltaObjectToString()).endTime;
        System.out.println(startTime + " - " + endTime);

        OverflowSeriesDataSource overflowSeriesDataSource = genDataSource(path);

//        PriorityTimeValuePairReader reader1 = new PriorityTimeValuePairReader(seriesInTsFileReader, new PriorityTimeValuePairReader.Priority(2));
//        PriorityTimeValuePairReader reader2 = new PriorityTimeValuePairReader(overflowInsertDataReader, new PriorityTimeValuePairReader.Priority(1));
//        TimeValuePairReader reader = new PriorityMergeSortTimeValuePairReader(reader1);

//        Directories.getInstance().setFolderForTest("");
//        Filter<?> filter = FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
        Filter<?> filter = TimeFilter.gtEq(0L);
        SeriesFilter<?> seriesFilter = new SeriesFilter<>(path, filter);
        IntervalFileNode intervalFileNode = new IntervalFileNode(null, tsfilePath);
        System.out.println(intervalFileNode.getFilePath());
        TimeValuePairReader reader = SeriesReaderFactory.getInstance().createSeriesReaderForMerge(intervalFileNode, overflowSeriesDataSource, seriesFilter);

        int count = 0;
        while (reader.hasNext()) {
            count++;
            reader.next();
        }
        System.out.println(count);
    }

    @Test
    public void loadRowGroupMetadata() throws IOException {
        List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(tsfilePath);
        TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);
        for (String deltaObjectID : tsFileMetaData.getDeltaObjectMap().keySet()) {
            TsDeltaObject deltaObject = tsFileMetaData.getDeltaObject(deltaObjectID);
            TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
            rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(randomAccessFileReader,
                    deltaObject.offset, deltaObject.metadataBlockSize));
            rowGroupMetaDatas.addAll(rowGroupBlockMetaData.getRowGroups());
        }
        randomAccessFileReader.close();
    }

    @Test
    public void testLoadMetaDataByDeltaObject() throws IOException {
        long startTime = System.currentTimeMillis();
        int count = 6000;

        for (int i = 0; i < count; i++) {
            ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(tsfilePath);
            TsFileMetaData tsFileMetaData = getTsFileMetadata(randomAccessFileReader);

            String deltaObjectID = new StringBuilder("root.performf.group_23.d_").append(2300 + i / 60).toString();
            TsDeltaObject deltaObject = tsFileMetaData.getDeltaObject(deltaObjectID);
            TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
            rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(randomAccessFileReader,
                    deltaObject.offset, deltaObject.metadataBlockSize));
            randomAccessFileReader.close();
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }

        System.out.println("Time used:" + (System.currentTimeMillis() - startTime) + "ms");
    }

    @Test
    public void writeTsFileTest() throws WriteProcessException, IOException {
        long startTime = System.currentTimeMillis();
        FileSchema fileSchema = new FileSchema();
        for (int i = 0; i < 60; i++) {
            fileSchema.registerMeasurement(new MeasurementDescriptor("s_" + i, TSDataType.DOUBLE, TSEncoding.RLE));
        }
        File file = new File("/users/zhangjinrui/Desktop/readTest/out");
        TsFileWriter fileWriter = new TsFileWriter(file, fileSchema, TSFileDescriptor.getInstance().getConfig());

        for (int i = 0; i < 100; i++) {
            int count = 0;
            for (int j = 0; j < 60; j++) {
                String deviceId = new StringBuilder("root.performf.group_23.d_").append(2300 + i).toString();
                String sensorId = "s_" + j;

                SeriesFilter<Long> seriesFilter = new SeriesFilter<>(new Path(deviceId + "." + sensorId), TimeFilter.gt(0L));
                ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(tsfilePath);
                SeriesReader seriesInTsFileReader = new SeriesReaderFromSingleFileWithFilterImpl(
                        randomAccessFileReader, seriesFilter.getSeriesPath(), seriesFilter.getFilter());

                while (seriesInTsFileReader.hasNext()) {
                    TimeValuePair timeValuePair = seriesInTsFileReader.next();
//                    TSRecord record = constructTsRecord(timeValuePair, deviceId, sensorId);
//                    fileWriter.write(record);
                    count++;
                }
                randomAccessFileReader.close();
            }
            System.out.println("count = " + count + " Time used:" + (System.currentTimeMillis() - startTime) + "ms");
        }
        fileWriter.close();
    }

    @Test
    public void writeTsFileUsingNewGenSeriesReaderTest() throws WriteProcessException, IOException {
        long startTime = System.currentTimeMillis();
        FileSchema fileSchema = new FileSchema();
        for (int i = 0; i < 60; i++) {
            fileSchema.registerMeasurement(new MeasurementDescriptor("s_" + i, TSDataType.DOUBLE, TSEncoding.RLE));
        }
        File file = new File("/users/zhangjinrui/Desktop/readTest/out");
        TsFileWriter fileWriter = new TsFileWriter(file, fileSchema, TSFileDescriptor.getInstance().getConfig());

        long count = 0;
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 60; j++) {
                String deviceId = new StringBuilder("root.performf.group_7.d_").append(700 + i).toString();
                String sensorId = "s_" + j;

                SeriesFilter<Long> seriesFilter = new SeriesFilter<>(new Path(deviceId + "." + sensorId), TimeFilter.gt(0L));
                SeriesReader seriesInTsFileReader = SeriesReaderFactory.getInstance().genTsFileSeriesReader(tsfilePath, seriesFilter);


                while (seriesInTsFileReader.hasNext()) {
                    TimeValuePair timeValuePair = seriesInTsFileReader.next();
                    TSRecord record = constructTsRecord(timeValuePair, deviceId, sensorId);
                    fileWriter.write(record);
                    count++;
                }
                seriesInTsFileReader.close();
            }
            System.out.println(i + " count = " + count + " Time used:" + (System.currentTimeMillis() - startTime) + "ms");
        }
        fileWriter.close();
    }

    private TSRecord constructTsRecord(TimeValuePair timeValuePair, String deltaObjectId, String measurementId) {
        TSRecord record = new TSRecord(timeValuePair.getTimestamp(), deltaObjectId);
        record.addTuple(DataPoint.getDataPoint(timeValuePair.getValue().getDataType(), measurementId,
                timeValuePair.getValue().getValue().toString()));
        return record;
    }


    public void readTsFile() throws IOException {
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(tsfilePath);
        SeriesReader reader = new SeriesReaderFromSingleFileWithoutFilterImpl(randomAccessFileReader, path);

        int i = 0;
        TimeValuePair lastTimeValuePair = null;
        TimeValuePair timeValuePair = null;
        while (reader.hasNext()) {
            lastTimeValuePair = timeValuePair;
            timeValuePair = reader.next();
            if (i == 0) {
                System.out.println("Min: " + timeValuePair.getTimestamp());
            }
            i++;
        }
        System.out.println("Max:" + lastTimeValuePair.getTimestamp());
        System.out.println("Max:" + timeValuePair.getTimestamp());
        System.out.println("Count = " + i);
    }

    public void readUnseqTsFile() throws IOException {
        OverflowSeriesDataSource overflowSeriesDataSource = genDataSource(path);
        OverflowInsertDataReader overflowInsertDataReader = SeriesReaderFactory.getInstance().createSeriesReaderForOverflowInsert(overflowSeriesDataSource);
        printTimestamp(overflowInsertDataReader);
    }

    private void printTimestamp(TimeValuePairReader reader) throws IOException {
        TimeValuePair timeValuePair = null;
        int i = 0;
        while (reader.hasNext()) {
            timeValuePair = reader.next();
            if (i == 0) {
                System.out.println("Min: " + timeValuePair.getTimestamp());
            }
            i++;
        }
        System.out.println("Max:" + timeValuePair.getTimestamp());
    }

    private OverflowSeriesDataSource genDataSource(Path path) throws IOException {
        File file = new File(unseqTsfilePath);
        OverflowIO overflowIO = new OverflowIO(unseqTsfilePath, file.length(), true);
        Map<String, Map<String, List<TimeSeriesChunkMetaData>>> metadata = readMetadata(overflowIO);
        OverflowSeriesDataSource overflowSeriesDataSource = new OverflowSeriesDataSource(path);
        OverflowInsertFile overflowInsertFile = new OverflowInsertFile();
        overflowInsertFile.setPath(unseqTsfilePath);
        overflowInsertFile.setTimeSeriesChunkMetaDatas(metadata.get(path.getDeltaObjectToString()).get(path.getMeasurementToString()));
        List<OverflowInsertFile> overflowInsertFileList = new ArrayList<>();
        overflowInsertFileList.add(overflowInsertFile);
        overflowSeriesDataSource.setOverflowInsertFileList(overflowInsertFileList);
        overflowSeriesDataSource.setRawSeriesChunk(null);
        return overflowSeriesDataSource;
    }

    private TsFileMetaData getTsFileMetadata(ITsRandomAccessFileReader randomAccessFileReader) throws IOException {
        long l = randomAccessFileReader.length();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        randomAccessFileReader.read(buf, 0, buf.length);
        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        return new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));
    }

    private Map<String, Map<String, List<TimeSeriesChunkMetaData>>> readMetadata(OverflowIO insertIO) throws IOException {

        Map<String, Map<String, List<TimeSeriesChunkMetaData>>> insertMetadatas = new HashMap<>();
        // read insert meta-data
        insertIO.toTail();
        long position = insertIO.getPos();
        while (position != 0) {
            insertIO.getReader().seek(position - FOOTER_LENGTH);
            int metadataLength = insertIO.getReader().readInt();
            byte[] buf = new byte[metadataLength];
            insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength);
            insertIO.getReader().read(buf, 0, buf.length);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
            RowGroupBlockMetaData rowGroupBlockMetaData = ReadWriteThriftFormatUtils
                    .readRowGroupBlockMetaData(inputStream);
            TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
            blockMeta.convertToTSF(rowGroupBlockMetaData);
            byte[] bytesPosition = new byte[8];
            insertIO.getReader().seek(position - FOOTER_LENGTH - metadataLength - POS_LENGTH);
            insertIO.getReader().read(bytesPosition, 0, POS_LENGTH);
            position = BytesUtils.bytesToLong(bytesPosition);
            for (RowGroupMetaData rowGroupMetaData : blockMeta.getRowGroups()) {
                String deltaObjectId = rowGroupMetaData.getDeltaObjectID();
                if (!insertMetadatas.containsKey(deltaObjectId)) {
                    insertMetadatas.put(deltaObjectId, new HashMap<>());
                }
                for (TimeSeriesChunkMetaData chunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
                    String measurementId = chunkMetaData.getProperties().getMeasurementUID();
                    if (!insertMetadatas.get(deltaObjectId).containsKey(measurementId)) {
                        insertMetadatas.get(deltaObjectId).put(measurementId, new ArrayList<>());
                    }
                    insertMetadatas.get(deltaObjectId).get(measurementId).add(0, chunkMetaData);
                }
            }
        }
        return insertMetadatas;
    }
}
