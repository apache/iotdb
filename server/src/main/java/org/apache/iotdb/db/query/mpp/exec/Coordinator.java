package org.apache.iotdb.db.query.mpp.exec;

import org.apache.iotdb.db.query.mpp.common.QueryId;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The coordinator for MPP.
 * It manages all the queries which are executed in current Node. And it will be responsible for the lifecycle of a query.
 * A query request will be represented as a QueryExecution.
 */
public class Coordinator {

    private ConcurrentHashMap<QueryId, QueryExecution> queryExecutionMap;

    private QueryExecution createQueryExecution() {
        return null;
    }

    private QueryExecution getQueryExecutionById() {
        return null;
    }

//    private TQueryResponse executeQuery(TQueryRequest request) {
//
//    }
}

