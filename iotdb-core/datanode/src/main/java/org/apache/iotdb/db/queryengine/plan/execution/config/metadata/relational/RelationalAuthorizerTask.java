package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class RelationalAuthorizerTask implements IConfigTask {
  private final RelationalAuthorStatement statement;

  public RelationalAuthorizerTask(RelationalAuthorStatement statement) {
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
