package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
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
