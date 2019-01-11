import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.query.executor.EngineQueryRouter;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.read.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Delete this class when submitting pr.
 */
public class PerformanceTest {

    private static int deviceStart = 9, deviceEnd = 9;
    private static int sensorStart = 9, sensorEnd = 9;

    public static void main(String[] args) throws IOException, FileNodeManagerException {

        //singleWithoutFilterTest();

        //queryMultiSeriesWithoutFilterTest();

        queryMultiSeriesWithFilterTest();
    }

    private static void singleWithoutFilterTest() throws IOException, FileNodeManagerException {

        List<Path> selectedPathList = new ArrayList<>();
        selectedPathList.add(getPath(1, 1));

        QueryExpression queryExpression = QueryExpression.create(selectedPathList, null);

        EngineQueryRouter queryRouter = new EngineQueryRouter();

        long startTime = System.currentTimeMillis();

        QueryDataSet queryDataSet = queryRouter.query(queryExpression);

        int count = 0;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            count++;
            //output(count, rowRecord, true);
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));

    }

    public static void queryMultiSeriesWithoutFilterTest() throws IOException, FileNodeManagerException {

        List<Path> selectedPathList = new ArrayList<>();
        for (int i = deviceStart; i <= deviceEnd; i++) {
            for (int j = sensorStart; j <= sensorEnd; j++) {
                selectedPathList.add(getPath(i, j));
            }
        }

        QueryExpression queryExpression = QueryExpression.create(selectedPathList, null);

        EngineQueryRouter queryRouter = new EngineQueryRouter();

        long startTime = System.currentTimeMillis();

        QueryDataSet queryDataSet = queryRouter.query(queryExpression);

        int count = 0;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            count++;
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));

    }

    public static void queryMultiSeriesWithFilterTest() throws IOException, FileNodeManagerException {

        List<Path> selectedPathList = new ArrayList<>();
        for (int i = deviceStart; i <= deviceEnd; i++) {
            for (int j = sensorStart; j <= sensorEnd; j++) {
                selectedPathList.add(getPath(i, j));
            }
        }

        Filter valueFilter = ValueFilter.gtEq(34300.0);
        Filter timeFilter = FilterFactory.and(TimeFilter.gtEq(1536396840000L), TimeFilter.ltEq(1537736665000L));

        IExpression expression = new SingleSeriesExpression(getPath(9, 9), timeFilter);
        EngineQueryRouter queryRouter = new EngineQueryRouter();

        QueryExpression queryExpression = QueryExpression.create(selectedPathList, expression);
        long startTime = System.currentTimeMillis();

        QueryDataSet queryDataSet = queryRouter.query(queryExpression);

        int count = 0;
        while (queryDataSet.hasNext()) {
            RowRecord rowRecord = queryDataSet.next();
            count++;
//            if (count % 10000 == 0)
//                System.out.println(rowRecord);
        }

        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Time consume : %s, count number : %s", endTime - startTime, count));

    }

    public static void output(int cnt, RowRecord rowRecord, boolean flag) {
        if (!flag) {
            return;
        }

        if (cnt % 10000 == 0) {
            System.out.println(cnt + " : " + rowRecord);
        }

        if (cnt > 97600) {
            System.out.println("----" + cnt + " : " + rowRecord);
        }
    }

    public static Path getPath(int d, int s) {
        return new Path(String.format("root.perform.group_0.d_%s.s_%s", d, s));
    }

}
