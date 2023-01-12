package org.apache.iotdb;

import org.apache.iotdb.db.qp.sql.OldIoTDBSqlParser12BaseVisitor;
import org.apache.iotdb.db.qp.sql.OldIoTDBSqlParser12Parser;

import java.util.List;

public class OldIoTDBSqlVisitor12 extends OldIoTDBSqlParser12BaseVisitor<String[]> {
  @Override
  public String[] visitPrefixPath(OldIoTDBSqlParser12Parser.PrefixPathContext ctx) {
    List<OldIoTDBSqlParser12Parser.NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size() + 1];
    path[0] = ctx.ROOT().getText();
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i + 1] = nodeNames.get(i).getText();
    }
    return path;
  }
}
