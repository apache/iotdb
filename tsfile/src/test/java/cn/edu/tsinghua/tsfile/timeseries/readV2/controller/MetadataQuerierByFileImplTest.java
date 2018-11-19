package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MetadataQuerierByFileImplTest {

    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private ITsRandomAccessFileReader randomAccessFileReader;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TsFileGeneratorForTest.generateFile(1000000, 1024 * 1024, 10000);
    }

    @After
    public void after() throws IOException {
        randomAccessFileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void test() throws IOException {
        randomAccessFileReader = new TsRandomAccessLocalFileReader(FILE_PATH);
        MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(randomAccessFileReader);
        List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = metadataQuerierByFile.getSeriesChunkDescriptorList(new Path("d2.s1"));
    }
}
