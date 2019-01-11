package cn.edu.tsinghua.tsfile.read.query.executor;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.GlobalTimeExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.utils.Binary;
import cn.edu.tsinghua.tsfile.read.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.read.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.expression.impl.BinaryExpression;
import cn.edu.tsinghua.tsfile.read.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.utils.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class QueryExecutorTest {


    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private TsFileSequenceReader fileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private ChunkLoader chunkLoader;
    private int rowCount = 10000;
    private TsFileExecutor queryExecutorWithQueryFilter;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
        fileReader = new TsFileSequenceReader(FILE_PATH);
        metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
        chunkLoader = new ChunkLoaderImpl(fileReader);
        queryExecutorWithQueryFilter = new TsFileExecutor(metadataQuerierByFile, chunkLoader);
    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void query1() throws IOException {
        Filter filter = TimeFilter.lt(1480562618100L);
        Filter filter2 = ValueFilter.gt(new Binary("dog"));

        IExpression IExpression = BinaryExpression.and(
                new SingleSeriesExpression(new Path("d1.s1"), filter),
                new SingleSeriesExpression(new Path("d1.s4"), filter2)
        );

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"))
                .setExpression(IExpression);
        long startTimestamp = System.currentTimeMillis();
        QueryDataSet queryDataSet = queryExecutorWithQueryFilter.execute(queryExpression);
        long aimedTimestamp = 1480562618000L;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            System.out.println(rowRecord);
            aimedTimestamp += 8;
        }
        long endTimestamp = System.currentTimeMillis();
        System.out.println("[Query]:" + queryExpression + "\n[Time]: " + (endTimestamp - startTimestamp) + "ms");
    }

    @Test
    public void queryWithoutFilter() throws IOException {
        QueryExecutor queryExecutor = new TsFileExecutor(metadataQuerierByFile, chunkLoader);

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s3"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"));

        long aimedTimestamp = 1480562618000L;
        int count = 0;
        long startTimestamp = System.currentTimeMillis();
        QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            aimedTimestamp++;
            count++;
        }
        Assert.assertEquals(rowCount, count);
        long endTimestamp = System.currentTimeMillis();
        System.out.println("[Query]:" + queryExpression + "\n[Time]: " + (endTimestamp - startTimestamp) + "ms");
    }

    @Test
    public void queryWithGlobalTimeFilter() throws IOException {
        QueryExecutor queryExecutor = new TsFileExecutor(metadataQuerierByFile, chunkLoader);

        IExpression IExpression = new GlobalTimeExpression(FilterFactory.and(TimeFilter.gtEq(1480562618100L), TimeFilter.lt(1480562618200L)));
        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s3"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"))
                .setExpression(IExpression);


        long aimedTimestamp = 1480562618100L;
        int count = 0;
        long startTimestamp = System.currentTimeMillis();
        QueryDataSet queryDataSet = queryExecutor.execute(queryExpression);
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            aimedTimestamp++;
            count++;
        }
        Assert.assertEquals(100, count);
        long endTimestamp = System.currentTimeMillis();
        System.out.println("[Query]:" + queryExpression + "\n[Time]: " + (endTimestamp - startTimestamp) + "ms");
    }
}
