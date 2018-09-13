package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;

import java.io.IOException;

public interface QueryExecutor {
    QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException;
}