package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

public class ShowClusterStatement extends ShowStatement implements IConfigStatement {

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowCluster(this, context);
  }

}
