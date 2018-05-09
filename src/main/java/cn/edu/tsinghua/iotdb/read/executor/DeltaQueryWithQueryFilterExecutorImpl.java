package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;

import java.io.IOException;

public class DeltaQueryWithQueryFilterExecutorImpl implements QueryExecutor {

    public DeltaQueryWithQueryFilterExecutorImpl() {}

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {

        return null;
    }
}
