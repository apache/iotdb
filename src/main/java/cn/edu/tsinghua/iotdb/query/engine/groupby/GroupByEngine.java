package cn.edu.tsinghua.iotdb.query.engine.groupby;

import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Group by aggregation implementation.
 */
public class GroupByEngine {

    // ThreadLocal<>

    public static QueryDataSet groupBy(List<Pair<Path, String>> aggres, List<FilterStructure> filterStructures,
                                       long unit, long origin, FilterExpression intervals, int fetchSize) {
        List<String> deltaObjectList = new ArrayList<>();
        List<String> measurementList = new ArrayList<>();
        for (Pair<Path, String> pair : aggres) {
            deltaObjectList.add(pair.left.getDeltaObjectToString());
            measurementList.add(pair.left.getMeasurementToString());
        }
        QueryDataSet groupByQueryDataSet = new GroupByQueryDataSet(deltaObjectList, measurementList);

        return null;
    }

}
