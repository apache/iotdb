package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ReaderByTimestampTest {

    private static final String FILE_PATH = TsFileGeneratorForSeriesReaderByTimestamp.outputDataFile;
    private TsFileSequenceReader fileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private int rowCount = 1000000;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForSeriesReaderByTimestamp.generateFile(rowCount, 10 * 1024 * 1024, 10000);
        fileReader = new TsFileSequenceReader(FILE_PATH);//TODO remove this class
        metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);

    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForSeriesReaderByTimestamp.after();
    }

    @Test
    public void readByTimestamp() throws IOException {
        ChunkLoaderImpl seriesChunkLoader = new ChunkLoaderImpl(fileReader);
        List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1.s1"));
        FileSeriesReader seriesReader = new FileSeriesReaderWithoutFilter(seriesChunkLoader, chunkMetaDataList);

        List<Long> timeList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        int count = 0;
        BatchData data = null;

        while (seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                timeList.add(data.currentTime() - 1);
                valueList.add(null);
                timeList.add(data.currentTime());
                valueList.add(data.currentValue());
                data.next();
                count++;
            }
        }

        long startTimestamp = System.currentTimeMillis();
        count = 0;

        SeriesReaderByTimestamp seriesReaderFromSingleFileByTimestamp = new SeriesReaderByTimestamp(seriesChunkLoader, chunkMetaDataList);

        for (long time : timeList) {
            Object value = seriesReaderFromSingleFileByTimestamp.getValueInTimestamp(time);
            if (value == null)
                Assert.assertNull(valueList.get(count));
            else
                Assert.assertEquals(valueList.get(count), value);
            count++;
        }
        long endTimestamp = System.currentTimeMillis();
        System.out.println("SeriesReadWithFilterTest. [Time used]: " + (endTimestamp - startTimestamp) +
                " ms. [Read Count]: " + count);
    }
}
