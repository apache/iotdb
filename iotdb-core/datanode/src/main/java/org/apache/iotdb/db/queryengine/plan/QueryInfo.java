package org.apache.iotdb.db.queryengine.plan;

public class QueryInfo {
  private final String queryId;
  private final long startTime;
  private final long endTime;
  private final long costTime;
  private final String statement;
  private final String user;
  private final String clientHost;

  public QueryInfo(
      String queryId,
      long startTime,
      long endTime,
      long costTime,
      String statement,
      String user,
      String clientHost) {
    this.queryId = queryId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.costTime = costTime;
    this.statement = statement;
    this.user = user;
    this.clientHost = clientHost;
  }

  public String getQueryId() {
    return queryId;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getCostTime() {
    return costTime;
  }

  public String getStatement() {
    return statement;
  }

  public String getUser() {
    return user;
  }

  public String getClientHost() {
    return clientHost;
  }
}
