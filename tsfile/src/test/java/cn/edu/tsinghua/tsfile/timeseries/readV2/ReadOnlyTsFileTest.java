package cn.edu.tsinghua.tsfile.timeseries.readV2;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
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
import cn.edu.tsinghua.tsfile.timeseries.readV2.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class ReadOnlyTsFileTest {

    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private ITsRandomAccessFileReader randomAccessFileReader;
    private int rowCount = 1000;
    private ReadOnlyTsFile tsFile;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForTest.generateFile(rowCount, 16 * 1024 * 1024, 10000);
        randomAccessFileReader = new TsRandomAccessLocalFileReader(FILE_PATH);
        tsFile = new ReadOnlyTsFile(randomAccessFileReader);
    }

    @After
    public void after() throws IOException {
        tsFile.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void queryTest() throws IOException {
        Filter<Integer> filter = TimeFilter.lt(1480562618100L);
        Filter<Binary> filter2 = ValueFilter.gt(new Binary("dog"));
        Filter<Long> filter3 = FilterFactory.and(TimeFilter.gtEq(1480562618000L), TimeFilter.ltEq(1480562618100L));

        QueryFilter queryFilter = QueryFilterFactory.or(
                QueryFilterFactory.and(
                        new SeriesFilter<>(new Path("d1.s1"), filter),
                        new SeriesFilter<>(new Path("d1.s4"), filter2)),
                new GlobalTimeFilter(filter3)
        );

        QueryExpression queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s4"))
                .setQueryFilter(queryFilter);
        QueryDataSet queryDataSet = tsFile.query(queryExpression);
        long aimedTimestamp = 1480562618000L;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            aimedTimestamp++;
        }

        queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s4"));
        queryDataSet = tsFile.query(queryExpression);
        aimedTimestamp = 1480562618000L;
        int count = 0;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            aimedTimestamp++;
            count++;
        }
        Assert.assertEquals(rowCount, count);

        queryExpression = QueryExpression.create()
                .addSelectedPath(new Path("d1.s1"))
                .addSelectedPath(new Path("d1.s4"))
                .setQueryFilter(new GlobalTimeFilter(filter3));
        queryDataSet = tsFile.query(queryExpression);
        aimedTimestamp = 1480562618000L;
        count = 0;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            Assert.assertEquals(aimedTimestamp, rowRecord.getTimestamp());
            aimedTimestamp++;
            count++;
        }
        Assert.assertEquals(101, count);

    }
}
