package cn.edu.tsinghua.tsfile.read.query.executor;

import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;


public interface QueryExecutor {

    QueryDataSet execute(QueryExpression queryExpression) throws IOException;
}
