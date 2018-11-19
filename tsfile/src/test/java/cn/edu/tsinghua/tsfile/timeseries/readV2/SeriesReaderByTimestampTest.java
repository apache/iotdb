package cn.edu.tsinghua.tsfile.timeseries.readV2;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithoutFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderByTimestampTest {

    private static final String FILE_PATH = TsFileGeneratorForSeriesReaderByTimestamp.outputDataFile;
    private ITsRandomAccessFileReader randomAccessFileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private int rowCount = 1000000;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForSeriesReaderByTimestamp.generateFile(rowCount, 10 * 1024 * 1024, 10000);
        randomAccessFileReader = new TsRandomAccessLocalFileReader(FILE_PATH);
        metadataQuerierByFile = new MetadataQuerierByFileImpl(randomAccessFileReader);
    }

    @After
    public void after() throws IOException {
        randomAccessFileReader.close();
        TsFileGeneratorForSeriesReaderByTimestamp.after();
    }

    @Test
    public void readByTimestamp() throws IOException {
        SeriesChunkLoaderImpl seriesChunkLoader = new SeriesChunkLoaderImpl(randomAccessFileReader);
        List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = metadataQuerierByFile.getSeriesChunkDescriptorList(new Path("d1.s1"));
        SeriesReader seriesReader = new SeriesReaderFromSingleFileWithoutFilterImpl(seriesChunkLoader, encodedSeriesChunkDescriptorList);

        List<TimeValuePair> timeValuePairList = new ArrayList<>();
        int count = 0;
        while (seriesReader.hasNext()) {
            TimeValuePair timeValuePair = seriesReader.next();
            if (count % 100 == 0) {
                timeValuePairList.add(new TimeValuePair(timeValuePair.getTimestamp() - 1, null));
                timeValuePairList.add(timeValuePair);
            }
            count++;
        }

        long startTimestamp = System.currentTimeMillis();
        count = 0;

        SeriesReaderFromSingleFileByTimestampImpl seriesReaderFromSingleFileByTimestamp = new SeriesReaderFromSingleFileByTimestampImpl(seriesChunkLoader, encodedSeriesChunkDescriptorList);

        for (TimeValuePair timeValuePair : timeValuePairList) {
            TsPrimitiveType value = seriesReaderFromSingleFileByTimestamp.getValueInTimestamp(timeValuePair.getTimestamp());
            Assert.assertEquals(timeValuePair.getValue(), value);
            count ++;
        }
        long endTimestamp = System.currentTimeMillis();
        System.out.println("SeriesReadWithFilterTest. [Time used]: " + (endTimestamp - startTimestamp) +
                " ms. [Read Count]: " + count);
    }
}
