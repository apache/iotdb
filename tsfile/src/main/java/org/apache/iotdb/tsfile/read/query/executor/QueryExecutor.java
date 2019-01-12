package org.apache.iotdb.tsfile.read.query.executor;

import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;

import java.io.IOException;


public interface QueryExecutor {

    QueryDataSet execute(QueryExpression queryExpression) throws IOException;
}
