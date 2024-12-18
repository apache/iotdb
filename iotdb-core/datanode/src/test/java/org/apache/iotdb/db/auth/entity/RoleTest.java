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

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RoleTest {

  @Test
  public void testRole_InitAndSerialize() throws IllegalPathException {
    Role role = new Role("role");
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.ln"));
    role.setPrivilegeList(Collections.singletonList(pathPrivilege));
    role.addPathPrivilege(new PartialPath("root.ln"), 1, false);
    role.addPathPrivilege(new PartialPath("root.ln"), 2, true);
    assertEqualsDeserializedRole(
        "Role{name='role', pathPrivilegeList=[root.ln : WRITE_DATA"
            + " READ_SCHEMA_with_grant_option], systemPrivilegeSet=[]}",
        role.toString());
    Role role1 = new Role("role1");
    role1.deserialize(role.serialize());
    assertEqualsDeserializedRole(
        "Role{name='role', pathPrivilegeList=[root.ln : "
            + "WRITE_DATA READ_SCHEMA_with_grant_option], systemPrivilegeSet=[]}",
        role1.toString());

    Role admin = new Role("root");
    PartialPath rootPath = new PartialPath(IoTDBConstant.PATH_ROOT + ".**");
    PathPrivilege pathPri = new PathPrivilege(rootPath);
    for (PrivilegeType item : PrivilegeType.values()) {
      if (!item.isPathRelevant()) {
        admin.getSysPrivilege().add(item.ordinal());
        admin.getSysPriGrantOpt().add(item.ordinal());
      } else {
        pathPri.grantPrivilege(item.ordinal(), true);
      }
    }
    admin.getPathPrivilegeList().add(pathPri);
    assertEqualsDeserializedRole(
        "Role{name='root', pathPrivilegeList=[root.** : READ_DAT"
            + "A_with_grant_option WRITE_DATA_with_grant_option READ_SCHEMA_with"
            + "_grant_option WRITE_SCHEMA_with_grant_option], systemPrivilegeSet=[MANAGE_ROLE"
            + "_with_grant_option , USE_UDF_with_grant_option , USE_CQ_with_grant_option , USE"
            + "_PIPE_with_grant_option , USE_TRIGGER_with_grant_option , MANAGE_DATABASE_with_g"
            + "rant_option , MANAGE_USER_with_grant_option , MAINTAIN_with_grant_option , EXTEND"
            + "_TEMPLATE_with_grant_option , USE_MODEL_with_grant_option ]}",
        admin.toString());
  }

  private void assertEqualsDeserializedRole(String expectedStr, String actualStr) {
    Assert.assertEquals(extractName(expectedStr), extractName(actualStr));
    List<String> expectedPathPrevilegeList = extractPathPrevilegeList(expectedStr);
    List<String> actualPathPrevilegeList = extractPathPrevilegeList(actualStr);
    Assert.assertTrue(
        expectedPathPrevilegeList.size() == actualPathPrevilegeList.size()
            && expectedPathPrevilegeList.containsAll(actualPathPrevilegeList)
            && actualPathPrevilegeList.containsAll(expectedPathPrevilegeList));
    Set<String> expectedSystemPrivilegeSet = extractSystemPrivilegeSet(expectedStr);
    Set<String> actualSystemPrivilegeSet = extractSystemPrivilegeSet(actualStr);
    Assert.assertTrue(
        expectedSystemPrivilegeSet.size() == actualSystemPrivilegeSet.size()
            && expectedSystemPrivilegeSet.containsAll(actualSystemPrivilegeSet)
            && actualSystemPrivilegeSet.containsAll(expectedSystemPrivilegeSet));
  }

  private String extractName(String roleStr) {
    Pattern namePattern = Pattern.compile("name='([^']+)'");
    Matcher nameMatcher = namePattern.matcher(roleStr);
    return nameMatcher.find() ? nameMatcher.group(1) : "";
  }

  private List<String> extractPathPrevilegeList(String roleStr) {
    Pattern pathPrivilegePattern = Pattern.compile("pathPrivilegeList=\\[([^]]+)\\]");
    Matcher pathPrivilegeMatcher = pathPrivilegePattern.matcher(roleStr);
    List<String> pathPrivilegeList = new ArrayList<>();
    if (pathPrivilegeMatcher.find()) {
      String[] privileges = pathPrivilegeMatcher.group(1).split("\\s+");
      pathPrivilegeList.addAll(Arrays.asList(privileges));
    }
    return pathPrivilegeList;
  }

  private Set<String> extractSystemPrivilegeSet(String roleStr) {
    Pattern systemPrivilegePattern = Pattern.compile("systemPrivilegeSet=\\[([^]]+)\\]");
    Matcher systemPrivilegeMatcher = systemPrivilegePattern.matcher(roleStr);
    Set<String> systemPrivilegeSet = new HashSet<>();
    if (systemPrivilegeMatcher.find()) {
      String[] privileges = systemPrivilegeMatcher.group(1).split(",\\s*");
      systemPrivilegeSet.addAll(Arrays.asList(privileges));
    }
    return systemPrivilegeSet;
  }
}
