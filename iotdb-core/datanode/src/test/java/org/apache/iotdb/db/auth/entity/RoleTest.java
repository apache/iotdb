/*
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
package org.apache.iotdb.db.auth.entity;

import org.apache.iotdb.commons.auth.entity.DatabasePrivilege;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.TablePrivilege;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class RoleTest {

  @Test
  public void testRole_InitAndSerialize() throws IllegalPathException {
    Role role = new Role("role");
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.ln"));
    role.setPrivilegeList(Collections.singletonList(pathPrivilege));
    role.grantPathPrivilege(new PartialPath("root.ln"), PrivilegeType.READ_SCHEMA, true);
    role.grantPathPrivilege(new PartialPath("root.ln"), PrivilegeType.READ_DATA, false);

    Assert.assertEquals(
        "Role{name='role', pathPrivilegeList=[root.ln : "
            + "READ_DATA READ_SCHEMA_with_grant_option], systemPrivilegeSet=[], "
            + "AnyScopePrivilegeMap=[], objectPrivilegeSet={}}",
        role.toString());
    Role role1 = new Role("role1");
    role1.deserialize(role.serialize());
    Assert.assertEquals(
        "Role{name='role', pathPrivilegeList=[root.ln : "
            + "READ_DATA READ_SCHEMA_with_grant_option], systemPrivilegeSet=[], "
            + "AnyScopePrivilegeMap=[], objectPrivilegeSet={}}",
        role1.toString());

    Role admin = new Role("root");
    PartialPath rootPath = new PartialPath(IoTDBConstant.PATH_ROOT + ".**");
    PathPrivilege pathPri = new PathPrivilege(rootPath);
    DatabasePrivilege databasePrivilege = new DatabasePrivilege("testDB");
    TablePrivilege tablePrivilege = new TablePrivilege("testTable");
    databasePrivilege.getTablePrivilegeMap().put("testTable", tablePrivilege);
    for (PrivilegeType item : PrivilegeType.values()) {
      if (item.isSystemPrivilege()) {
        admin.getSysPrivilege().add(item);
        admin.getSysPriGrantOpt().add(item);
      } else if (item.isPathPrivilege()) {
        pathPri.grantPrivilege(item, true);
      } else if (item.isRelationalPrivilege()) {
        databasePrivilege.grantDBPrivilege(item);
        databasePrivilege.grantDBGrantOption(item);
        databasePrivilege.grantTablePrivilege("testTable", item);
        databasePrivilege.grantTableGrantOption("testTable", item);
        admin.grantAnyScopePrivilege(item, true);
      }
    }
    admin.getDBScopePrivilegeMap().put("testDB", databasePrivilege);
    admin.getPathPrivilegeList().add(pathPri);
    Assert.assertEquals(
        "Role{name='root', pathPrivilegeList=[root.** : READ_DATA_with_grant_option WRITE_DATA_with_grant_option READ_SCHEMA_with_grant_option WRITE_SCHEMA_with_grant_option], systemPrivilegeSet=[USE_MODEL_with_grant_option, MAINTAIN_with_grant_option, EXTEND_TEMPLATE_with_grant_option, SYSTEM_with_grant_option, MANAGE_DATABASE_with_grant_option, MANAGE_USER_with_grant_option, USE_TRIGGER_with_grant_option, USE_CQ_with_grant_option, SECURITY_with_grant_option, USE_PIPE_with_grant_option, USE_UDF_with_grant_option, MANAGE_ROLE_with_grant_option, AUDIT_with_grant_option], AnyScopePrivilegeMap=[DELETE_with_grant_option, DROP_with_grant_option, ALTER_with_grant_option, CREATE_with_grant_option, SELECT_with_grant_option, INSERT_with_grant_option], objectPrivilegeSet={testDB=Database(testDB):{CREATE_with_grant_option,DROP_with_grant_option,ALTER_with_grant_option,SELECT_with_grant_option,INSERT_with_grant_option,DELETE_with_grant_option,; Tables: [ testTable(CREATE_with_grant_option,DROP_with_grant_option,ALTER_with_grant_option,SELECT_with_grant_option,INSERT_with_grant_option,DELETE_with_grant_option,)]}}}",
        admin.toString());
  }
}
