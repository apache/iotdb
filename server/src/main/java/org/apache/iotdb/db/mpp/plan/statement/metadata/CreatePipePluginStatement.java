package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class CreatePipePluginStatement extends Statement implements IConfigStatement {

  private final String pluginName;
  private final String className;
  private final String uriString;

  public CreatePipePluginStatement(String pluginName, String className, String uriString) {
    super();
    statementType = StatementType.CREATE_PIPEPLUGIN;
    this.pluginName = pluginName;
    this.className = className;
    this.uriString = uriString;
  }

  public String getPluginName() {
    return pluginName;
  }

  public String getClassName() {
    return className;
  }

  public String getUriString() {
    return uriString;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreatePipePlugin(this, context);
  }
}
