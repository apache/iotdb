package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class DeltaQueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaQueryEngine.class);
    private QueryExecutor queryExecutor;

    public DeltaQueryEngine() {}

    public QueryDataSet query(QueryExpression queryExpression) throws IOException {
        return queryExecutor.execute(queryExpression);
    }
}
