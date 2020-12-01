package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.RootOperator;

public class KillQueryOperator extends RootOperator {
  int queryId = -1;

  public KillQueryOperator(int tokenIntType) {
    this(tokenIntType, OperatorType.KILL);
  }

  public KillQueryOperator(int tokenIntType, OperatorType operatorType) {
    super(tokenIntType);
    this.operatorType = operatorType;
  }

  public void setQueryId(int queryId) {
    this.queryId = queryId;
  }

  public int getQueryId() {
    return queryId;
  }
}
