package org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.write;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.statement.sys.AuthorStatement;

import java.nio.ByteBuffer;
import java.util.List;

public class AuthorNode extends PlanNode {

  private AuthorStatement.AuthorType authorType;
  private String userName;
  private String roleName;
  private String password;
  private String newPassword;
  private String[] privilegeList;
  private PartialPath nodeName;

  public AuthorNode(
      PlanNodeId id,
      AuthorStatement.AuthorType authorType,
      String userName,
      String roleName,
      String password,
      String newPassword,
      String[] privilegeList,
      PartialPath nodeName) {
    super(id);
    this.authorType = authorType;
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.privilegeList = privilegeList;
    this.nodeName = nodeName;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChildren(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}
}
