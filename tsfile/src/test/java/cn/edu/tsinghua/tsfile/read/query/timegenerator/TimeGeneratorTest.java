package cn.edu.tsinghua.tsfile.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.BinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.utils.Binary;
import cn.edu.tsinghua.tsfile.read.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.read.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.utils.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class TimeGeneratorTest {

    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private TsFileSequenceReader fileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private ChunkLoader chunkLoader;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForTest.generateFile(1000, 10 * 1024 * 1024, 10000);
        fileReader = new TsFileSequenceReader(FILE_PATH);
        metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
        chunkLoader = new ChunkLoaderImpl(fileReader);
    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void testTimeGenerator() throws IOException {
        long startTimestamp = 1480562618000L;
        Filter filter = TimeFilter.lt(1480562618100L);
        Filter filter2 = ValueFilter.gt(new Binary("dog"));
        Filter filter3 = FilterFactory.and(TimeFilter.gtEq(1480562618000L), TimeFilter.ltEq(1480562618100L));

        IExpression IExpression = BinaryExpression.or(
                BinaryExpression.and(
                        new SingleSeriesExpression(new Path("d1.s1"), filter),
                        new SingleSeriesExpression(new Path("d1.s4"), filter2)
                ),
                new SingleSeriesExpression(new Path("d1.s1"), filter3));

        TimeGeneratorImpl timestampGenerator = new TimeGeneratorImpl(IExpression, chunkLoader, metadataQuerierByFile);
        while (timestampGenerator.hasNext()) {
//            System.out.println(timestampGenerator.next());
            Assert.assertEquals(startTimestamp, timestampGenerator.next());
            startTimestamp += 1;
        }
        Assert.assertEquals(1480562618101L, startTimestamp);
    }
}
