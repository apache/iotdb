package org.apache.iotdb.db.query.mpp.common;

/**
 * This class is used to record the context of a query including QueryId, query statement, session info and so on
 */
public class QueryContext {
    private String statement;
    private QueryId queryId;
    private QuerySession session;
}
