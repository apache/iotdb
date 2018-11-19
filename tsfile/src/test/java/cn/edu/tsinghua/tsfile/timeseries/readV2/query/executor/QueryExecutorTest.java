package cn.edu.tsinghua.tsfile.timeseries.readV2.query.executor;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileGeneratorForTest;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryWithQueryFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryWithoutFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class QueryExecutorTest {


    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private ITsRandomAccessFileReader randomAccessFileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private SeriesChunkLoader seriesChunkLoader;
    private int rowCount = 10000;
    private QueryWithQueryFilterExecutorImpl queryExecutorWithQueryFilter;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
        randomAccessFileReader = new TsRandomAccessLocalFileReader(FILE_PATH);
        metadataQuerierByFile = new MetadataQuerierByFileImpl(randomAccessFileReader);
        seriesChunkLoader = new SeriesChunkLoaderImpl(randomAccessFileReader);
        queryExecutorWithQueryFilter = new QueryWithQueryFilterExecutorImpl(seriesChunkLoader, metadataQuerierByFile);
    }

    @After
    public void after() throws IOException {
        randomAccessFileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void query1() throws IOException {
        Filter<Integer> filter = TimeFilter.lt(1480562618100L);
        Filter<Binary> filter2 = ValueFilter.gt(new Binary("dog"));
//        Filter<Integer> filter3 = FilterFactory.and(TimeFilter.gtEq(1480562618000L), TimeFilter.ltEq(1480562618100L));

        QueryFilter queryFilter = QueryFilterFactory.and(
                new SeriesFilter<>(new Path("d1.s1"), filter),
                new SeriesFilter<>(new Path("d1.s4"), filter2)
        );

//        QueryFilter queryFilter = new SeriesFilter<>(new SeriesDescriptor(new Path("d1.s1"), TSDataType.INT32), filter);

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"))
                .setQueryFilter(queryFilter);
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
        QueryExecutor queryExecutor = new QueryWithoutFilterExecutorImpl(seriesChunkLoader, metadataQuerierByFile);

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s2"))
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
        QueryExecutor queryExecutor = new QueryWithGlobalTimeFilterExecutorImpl(seriesChunkLoader, metadataQuerierByFile);

        QueryFilter queryFilter = new GlobalTimeFilter(FilterFactory.and(TimeFilter.gtEq(1480562618100L), TimeFilter.lt(1480562618200L)));
        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s2"))
                .addSelectedPath(new Path("d1.s4"))
                .addSelectedPath(new Path("d1.s5"))
                .setQueryFilter(queryFilter);


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
