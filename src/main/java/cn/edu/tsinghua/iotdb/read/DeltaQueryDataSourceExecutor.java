package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

public class DeltaQueryDataSourceExecutor {

    public static QueryDataSource getQueryDataSource(SeriesFilter<?> seriesFilter) {
        return null;
    }

    public static QueryDataSource getQueryDataSource(Path selectedPath) {
        return null;
    }
}
