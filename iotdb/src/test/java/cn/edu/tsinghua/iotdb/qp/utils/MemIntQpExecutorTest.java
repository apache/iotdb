package cn.edu.tsinghua.iotdb.qp.utils;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.FilterUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;

/**
 * 
 * @author kangrong
 *
 */
public class MemIntQpExecutorTest {
    private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());
    private Path path1;
    private Path path2;

    @Before
    public void before() throws ProcessorException {
        path1 =
                new Path(new StringContainer(new String[] {"device_1", "sensor_1"},
                        SystemConstant.PATH_SEPARATOR));
        path2 =
                new Path(new StringContainer(new String[] {"device_1", "sensor_2"},
                        SystemConstant.PATH_SEPARATOR));
        for (int i = 1; i <= 10; i++) {
            processor.getExecutor().insert(path1, i * 20, Integer.toString(i * 20 + 1));
            processor.getExecutor().insert(path2, i * 50, Integer.toString(i * 50 + 2));
        }
    }

    @Test
    public void testQueryWithoutFilter() throws ProcessorException {

        List<Path> pathList = new ArrayList<Path>();
        pathList.add(path1);
        pathList.add(path2);
        OnePassQueryDataSet ret = null;

        while (true) {
            ret = processor.getExecutor().query(0, pathList, null, null, null, 1, ret);
            if (!ret.hasNextRecord())
                break;
            while (ret.hasNextRecord()) {
                OldRowRecord r = ret.getNextRecord();
                System.out.println(r);
            }
        }
        System.out.println();
    }

    @Test
    public void testQueryWithFilter1() throws ProcessorException, IOException {

        List<Path> pathList = new ArrayList<Path>();
        pathList.add(path2);
        pathList.add(path1);

        OnePassQueryDataSet ret = null;
        String filterString = "2,device_1.sensor_1,(>=80)&(<=110)";
        // default filter type is integer
        SingleSeriesFilterExpression valueFilter = FilterUtils.construct(filterString, null);
        while (true) {
            ret = processor.getExecutor().query(0, pathList, null, null, valueFilter, 1, ret);
            if (!ret.hasNextRecord())
                break;
            while (ret.hasNextRecord()) {
                OldRowRecord r = ret.getNextRecord();
                System.out.println(r);
            }
        }
        System.out.println();
    }

    @Test
    public void testQueryWithFilter2() throws ProcessorException, IOException {
        List<Path> pathList = new ArrayList<Path>();
        // pathList.add(path1);
        pathList.add(path2);
        OnePassQueryDataSet ret = null;
        String filterString = "2,device_1.sensor_2,((>=100)&(<=200))";
        // default filter type is integer
        SingleSeriesFilterExpression valueFilter = FilterUtils.construct(filterString, null);
        while (true) {
            ret = processor.getExecutor().query(0, pathList, null, null, valueFilter, 1, ret);
            if (!ret.hasNextRecord())
                break;
            while (ret.hasNextRecord()) {
                OldRowRecord r = ret.getNextRecord();
                System.out.println(r);
            }
        }
    }

}
