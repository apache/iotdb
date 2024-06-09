package org.apache.iotdb.db.queryengine.plan.execution.config.sys;

import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.AuthTableStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class AuthorizerTableTask implements IConfigTask {
  private final AuthTableStatement statement;

  public AuthorizerTableTask(AuthTableStatement statement) {
    this.statement = statement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor) {
    if (statement.getQueryType() == QueryType.WRITE) {
      return AuthorityChecker.operatePermission(statement);
    } else {
      return AuthorityChecker.queryPermission(statement);
    }
  }
}
