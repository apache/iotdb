package cn.edu.tsinghua.iotdb.queryV2.factory;

import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.queryV2.TsFileGenerator;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zhangjinrui on 2018/1/19.
 */
public class SeriesReaderFactoryTest {

    @Test
    public void testJobId() throws InterruptedException, IOException {
        OverflowSeriesDataSource overflowSeriesDataSource = new OverflowSeriesDataSource(new Path("d1.s1"));
        overflowSeriesDataSource.setOverflowInsertFileList(new ArrayList<>());

        int count = 10;
        OverflowInsertDataReader seriesReader = SeriesReaderFactory.getInstance().createSeriesReaderForOverflowInsert(overflowSeriesDataSource);
        int currentJobId = seriesReader.getJobId().intValue();
        int[] map = new int[count + currentJobId];
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(0, map[i]);
        }

        Thread[] threads = new Thread[count];
        for (int i = 0; i < count; i++) {
            threads[i] = new OverflowJob(map, overflowSeriesDataSource);
            threads[i].start();
        }

        for (int i = 0; i < count; i++) {
            threads[i].join();
        }

        for (int i = currentJobId; i < count; i++) {
            Assert.assertEquals(1, map[i]);
        }
    }

    private class OverflowJob extends Thread {
        int[] map;
        OverflowSeriesDataSource overflowSeriesDataSource;

        public OverflowJob(int[] map, OverflowSeriesDataSource overflowSeriesDataSource) {
            this.map = map;
            this.overflowSeriesDataSource = overflowSeriesDataSource;
        }

        public void run() {
            try {
                OverflowInsertDataReader seriesReader = SeriesReaderFactory.getInstance().createSeriesReaderForOverflowInsert(overflowSeriesDataSource);
                synchronized (map) {
                    map[seriesReader.getJobId().intValue() - 1]++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCreate() throws InterruptedException, WriteProcessException, IOException {
        String path = "seriesFactoryTest.ts";
        TsFileGenerator.write(path, 10000, 1 * 1024 * 1024, 50000);
        ITsRandomAccessFileReader randomAccessFileReader = new TsRandomAccessLocalFileReader(path);
        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDatas = retrieveTimeSeriesChunkMetadata(randomAccessFileReader, new Path("d1.s1"));
        OverflowInsertFile overflowInsertFile = new OverflowInsertFile(path, timeSeriesChunkMetaDatas);

        List<OverflowInsertFile> overflowInsertFiles = new ArrayList<>();
        overflowInsertFiles.add(overflowInsertFile);

        OverflowSeriesDataSource overflowSeriesDataSource = new OverflowSeriesDataSource(new Path("d1.s1"));
        overflowSeriesDataSource.setOverflowInsertFileList(overflowInsertFiles);

        SeriesReader seriesReader = SeriesReaderFactory.getInstance().createSeriesReaderForOverflowInsert(overflowSeriesDataSource);

        long timestamp = TsFileGenerator.START_TIMESTAMP;
        while (seriesReader.hasNext()) {
            Assert.assertEquals(timestamp, seriesReader.next().getTimestamp());
            timestamp++;
        }
        seriesReader.close();

        Filter<Long> filter = TimeFilter.lt(TsFileGenerator.START_TIMESTAMP + 10);
        SeriesReader seriesReader2 = SeriesReaderFactory.getInstance().createSeriesReaderForOverflowInsert(overflowSeriesDataSource, filter);
        timestamp = TsFileGenerator.START_TIMESTAMP;
        while (seriesReader2.hasNext()) {
            Assert.assertEquals(timestamp, seriesReader2.next().getTimestamp());
            timestamp++;
        }
        Assert.assertEquals(10, timestamp - TsFileGenerator.START_TIMESTAMP);
        seriesReader2.close();
        randomAccessFileReader.close();
        TsFileGenerator.after();
    }


    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

    private List<TimeSeriesChunkMetaData> retrieveTimeSeriesChunkMetadata(ITsRandomAccessFileReader randomAccessFileReader, Path path) throws IOException {
        long l = randomAccessFileReader.length();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
        byte[] buf = new byte[fileMetaDataLength];
        randomAccessFileReader.read(buf, 0, buf.length);

        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        TsFileMetaData fileMetaData = new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));

        String deltaObjectID = path.getDeltaObjectToString();
        TsDeltaObject deltaObject = fileMetaData.getDeltaObject(deltaObjectID);
        TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
        rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(randomAccessFileReader,
                deltaObject.offset, deltaObject.metadataBlockSize));


        List<RowGroupMetaData> rowGroupMetaDataList = rowGroupBlockMetaData.getRowGroups();
        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = new ArrayList<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInOneRowGroup = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
            for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataListInOneRowGroup) {
                if (path.getMeasurementToString().equals(timeSeriesChunkMetaData.getProperties().getMeasurementUID())) {
                    timeSeriesChunkMetaDataList.add(timeSeriesChunkMetaData);
                }
            }
        }
        return timeSeriesChunkMetaDataList;
    }
}
