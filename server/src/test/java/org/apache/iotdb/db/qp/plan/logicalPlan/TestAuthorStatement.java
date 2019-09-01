package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestAuthorStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test
  public void testCreateUser() {
    RootOperator op = generator.getLogicalPlan("create user myname mypwd;");
    assertEquals(AuthorOperator.class, op.getClass());
    assertEquals(SQLConstant.TOK_AUTHOR_CREATE, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.CREATE_USER, ((AuthorOperator)op).getAuthorType());
    assertEquals("myname", ((AuthorOperator)op).getUserName());
    assertEquals("mypwd", ((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator) op).getRoleName());
    assertNull(((AuthorOperator) op).getNewPassword());
    assertNull(((AuthorOperator) op).getNodeName());
    assertNull(((AuthorOperator) op).getPrivilegeList());
  }

  @Test
  public void testDropUser(){
    RootOperator op = generator.getLogicalPlan("drop user myname");
    assertEquals(SQLConstant.TOK_AUTHOR_DROP, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.DROP_USER, ((AuthorOperator)op).getAuthorType());
    assertEquals("myname", ((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getPrivilegeList());
    assertNull(((AuthorOperator)op).getRoleName());
  }

  @Test
  public void testCreateRole() {
    RootOperator op = generator.getLogicalPlan("create role admin");
    assertEquals(SQLConstant.TOK_AUTHOR_CREATE, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.CREATE_ROLE, ((AuthorOperator)op).getAuthorType());
    assertEquals("admin", ((AuthorOperator)op).getRoleName());
    assertNull(((AuthorOperator)op).getPrivilegeList());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getUserName());
  }

  @Test
  public void testDropRole() {
    RootOperator op = generator.getLogicalPlan("drop role admin;");
    assertEquals(SQLConstant.TOK_AUTHOR_DROP, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.DROP_ROLE, ((AuthorOperator)op).getAuthorType());
    assertEquals("admin", ((AuthorOperator)op).getRoleName());
    assertNull(((AuthorOperator)op).getPrivilegeList());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getUserName());

  }

  @Test
  public void testGrantUser() {
    RootOperator op = generator.getLogicalPlan("grant user myusername privileges 'create','delete' on root.laptop.d1.s1");
    assertEquals(SQLConstant.TOK_AUTHOR_GRANT, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.GRANT_USER, ((AuthorOperator)op).getAuthorType());
    assertEquals("myusername", ((AuthorOperator)op).getUserName());
    String[] expectedPrivileges = new String[]{"create", "delete"};
    String[] privileges = ((AuthorOperator)op).getPrivilegeList();
    for(int i = 0; i < privileges.length; i++){
      assertEquals(expectedPrivileges[i], privileges[i]);
    }
    Path expectedPath = new Path("root.laptop.d1.s1");
    assertEquals(expectedPath,((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getRoleName());
  }

  @Test
  public void testGrantRole() {
    RootOperator op = generator.getLogicalPlan("grant role admin privileges 'create','delete' on root.laptop.d1.s1;");
    assertEquals(SQLConstant.TOK_AUTHOR_GRANT, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.GRANT_ROLE, ((AuthorOperator)op).getAuthorType());
    assertEquals("admin", ((AuthorOperator)op).getRoleName());
    String[] expectedPrivileges = new String[]{"create", "delete"};
    String[] privileges = ((AuthorOperator)op).getPrivilegeList();
    for(int i = 0; i < privileges.length; i++){
      assertEquals(expectedPrivileges[i], privileges[i]);
    }
    Path expectedPath = new Path("root.laptop.d1.s1");
    assertEquals(expectedPath,((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getUserName());
  }

  @Test
  public void testRevokeUser() {
    RootOperator op = generator.getLogicalPlan("revoke user myusername privileges 'create','delete' on root.laptop.d1.s1");
    assertEquals(SQLConstant.TOK_AUTHOR_REVOKE, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.REVOKE_USER, ((AuthorOperator)op).getAuthorType());
    assertEquals("myusername", ((AuthorOperator)op).getUserName());
    String[] expectedPrivileges = new String[]{"create", "delete"};
    String[] privileges = ((AuthorOperator)op).getPrivilegeList();
    for(int i = 0; i < privileges.length; i++){
      assertEquals(expectedPrivileges[i], privileges[i]);
    }
    Path expectedPath = new Path("root.laptop.d1.s1");
    assertEquals(expectedPath,((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getRoleName());
  }

  @Test
  public void testRevokeRole() {
    RootOperator op = generator.getLogicalPlan("revoke role admin privileges 'create','delete' on root.laptop.d1.s1;");
    assertEquals(SQLConstant.TOK_AUTHOR_REVOKE, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.REVOKE_ROLE, ((AuthorOperator)op).getAuthorType());
    assertEquals("admin", ((AuthorOperator)op).getRoleName());
    String[] expectedPrivileges = new String[]{"create", "delete"};
    String[] privileges = ((AuthorOperator)op).getPrivilegeList();
    for(int i = 0; i < privileges.length; i++){
      assertEquals(expectedPrivileges[i], privileges[i]);
    }
    Path expectedPath = new Path("root.laptop.d1.s1");
    assertEquals(expectedPath,((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getUserName());
  }

  @Test
  public void testGrantRoleToUser() {
    RootOperator op = generator.getLogicalPlan("grant admin to Tom");
    assertEquals(SQLConstant.TOK_AUTHOR_GRANT, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.GRANT_ROLE_TO_USER, ((AuthorOperator)op).getAuthorType());
    assertEquals("admin", ((AuthorOperator)op).getRoleName());
    assertEquals("Tom", ((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getPrivilegeList());
  }

  @Test
  public void testRevokeRoleFromUser() {
    RootOperator op = generator.getLogicalPlan("revoke admin from Tom;");
    assertEquals(SQLConstant.TOK_AUTHOR_REVOKE, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.REVOKE_ROLE_FROM_USER, ((AuthorOperator)op).getAuthorType());
    assertEquals("admin", ((AuthorOperator)op).getRoleName());
    assertEquals("Tom", ((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getPassWord());
    assertNull(((AuthorOperator)op).getNewPassword());
    assertNull(((AuthorOperator)op).getNodeName());
    assertNull(((AuthorOperator)op).getPrivilegeList());
  }
}
