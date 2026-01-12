package org.apache.iotdb.db.queryengine.plan;

public class StatedQueryInfo extends QueryInfo {
  private final String queryState;

  public StatedQueryInfo(
      String queryId,
      long startTime,
      long endTime,
      long costTime,
      String statement,
      String user,
      String clientHost,
      String queryState) {
    super(queryId, startTime, endTime, costTime, statement, user, clientHost);
    this.queryState = queryState;
  }

  public String getQueryState() {
    return queryState;
  }
}
