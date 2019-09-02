/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

public class TestListStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test
  public void listUser() {
    RootOperator op = generator.getLogicalPlan("list user;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_USER, ((AuthorOperator)op).getAuthorType());
  }

  @Test
  public void listRole() {
    RootOperator op = generator.getLogicalPlan("list role;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_ROLE, ((AuthorOperator)op).getAuthorType());
  }

  @Test
  public void listPrivileges() {
    RootOperator op = generator.getLogicalPlan("LIST PRIVILEGES USER sgcc_wirte_user ON root.sgcc;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_USER_PRIVILEGE, ((AuthorOperator)op).getAuthorType());
    assertEquals("sgcc_wirte_user", ((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getRoleName());
    assertEquals(new Path("root.sgcc"), ((AuthorOperator)op).getNodeName());
  }

  @Test
  public void listPrivilegesOnRolesOnSpecificPath() {
    RootOperator op = generator.getLogicalPlan("LIST PRIVILEGES ROLE wirte_role ON root.sgcc;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE, ((AuthorOperator)op).getAuthorType());
    assertEquals("wirte_role", ((AuthorOperator)op).getRoleName());
    assertNull(((AuthorOperator)op).getUserName());
    assertEquals(new Path("root.sgcc"), ((AuthorOperator)op).getNodeName());
  }

  @Test
  public void listUserPrivileges() {
    RootOperator op = generator.getLogicalPlan(" LIST USER PRIVILEGES tempuser;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_USER_PRIVILEGE, ((AuthorOperator)op).getAuthorType());
    assertEquals("tempuser", ((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getRoleName());
    assertNull(((AuthorOperator)op).getNodeName());
  }

  @Test
  public void listPrivilegesOfRoles() {
    RootOperator op = generator.getLogicalPlan("LIST ROLE PRIVILEGES actor;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE, ((AuthorOperator)op).getAuthorType());
    assertEquals("actor", ((AuthorOperator)op).getRoleName());
    assertNull(((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getNodeName());
  }

  @Test
  public void listRolesOfUsers() {
    RootOperator op = generator.getLogicalPlan(" LIST ALL ROLE OF USER tempuser;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_USER_ROLES, ((AuthorOperator)op).getAuthorType());
    assertEquals("tempuser", ((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getRoleName());
    assertNull(((AuthorOperator)op).getNodeName());
  }

  @Test
  public void listUsersOfRoles() {
    RootOperator op = generator.getLogicalPlan("LIST ALL USER OF ROLE roleuser;");
    assertEquals(SQLConstant.TOK_LIST, op.getTokenIntType());
    assertEquals(AuthorOperator.AuthorType.LIST_ROLE_USERS, ((AuthorOperator)op).getAuthorType());
    assertEquals("roleuser", ((AuthorOperator)op).getRoleName());
    assertNull(((AuthorOperator)op).getUserName());
    assertNull(((AuthorOperator)op).getNodeName());
  }
}
