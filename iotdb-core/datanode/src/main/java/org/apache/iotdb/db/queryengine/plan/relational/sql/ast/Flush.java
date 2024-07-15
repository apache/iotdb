package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

public class Flush extends WrappedStatement {

  public Flush(Statement innerTreeStatement, MPPQueryContext context) {
    super(innerTreeStatement, context);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFlush(this, context);
  }
}
