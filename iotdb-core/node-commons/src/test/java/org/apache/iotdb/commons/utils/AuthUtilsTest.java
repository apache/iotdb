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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AuthUtilsTest {

  @Test
  public void authUtilsTest_ParameterCheck() throws AuthException, IllegalPathException {
    AuthUtils.validatePassword("hello@123456");
    AuthUtils.validatePassword("hello$123456");
    AuthUtils.validatePassword("hello$^123456");
    AuthUtils.validatePassword("hel_lo$^123456");
    AuthUtils.validatePassword("he!l_lo$^123456");
    AuthUtils.validatePassword("he!l_l$o$^123456");
    AuthUtils.validatePassword("he!l_l!@#$%^*()_+-=$o$^");
    AuthUtils.validatePassword("he!l^^+=123456");
    AuthUtils.validatePassword("he!l*^^+=123456");
    AuthUtils.validatePassword("he!!l*^^+=123456");
    AuthUtils.validatePassword("he!!l*()^^+=");
    AuthUtils.validateUsername("!@#$%&^&*()_+-=");
    AuthUtils.validateUsername("!@!%^&!@#%$#@#$%&^&*()_+-=");
    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePassword("he!!l\\*()^^+="));
    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePassword("he!l^^ +="));
    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePassword("he"));
    Assert.assertThrows(
        AuthException.class,
        () ->
            AuthUtils.validatePassword(
                "heqwertyuiopasdfghjklzxcvbnm123456789999999asdfgh\"\n"
                    + "                + \"jkzxcvbnmqwertyuioasdfghjklzxcvbnm"));
  }

  @Test
  public void authUtilsTest_PrivilegeGrantRevokeCheck() throws IllegalPathException {
    PartialPath path = new PartialPath(new String("root.t1"));
    PathPrivilege pathWithPri = new PathPrivilege(path);
    pathWithPri.grantPrivilege(PrivilegeType.READ_SCHEMA, false);
    pathWithPri.grantPrivilege(PrivilegeType.READ_DATA, false);

    PartialPath path2 = new PartialPath(new String("root.t2"));
    PathPrivilege pathWithPri2 = new PathPrivilege(path2);
    pathWithPri2.grantPrivilege(PrivilegeType.WRITE_SCHEMA, false);

    PartialPath path3 = new PartialPath(new String("root.**"));
    PathPrivilege pathWithPri3 = new PathPrivilege(path3);
    pathWithPri3.grantPrivilege(PrivilegeType.READ_DATA, false);

    /** root.t1 : read schema, read data; root.t2 : write schema; root.** : read data */
    // Privilege list is empty.
    Assert.assertFalse(AuthUtils.checkPathPrivilege(path2, PrivilegeType.READ_SCHEMA, null));

    List<PathPrivilege> privilegeList = new ArrayList<>();
    privilegeList.add(pathWithPri);
    privilegeList.add(pathWithPri2);
    privilegeList.add(pathWithPri3);
    Assert.assertTrue(
        AuthUtils.checkPathPrivilege(path2, PrivilegeType.READ_SCHEMA, privilegeList));
    Assert.assertTrue(AuthUtils.checkPathPrivilege(path, PrivilegeType.READ_SCHEMA, privilegeList));

    pathWithPri.revokePrivilege(PrivilegeType.READ_SCHEMA);
    /** root.t1 : read data; root.t2 : write schema ; root.** : read data */
    Assert.assertFalse(
        AuthUtils.checkPathPrivilege(path, PrivilegeType.READ_SCHEMA, privilegeList));
    Assert.assertTrue(AuthUtils.checkPathPrivilege(path, PrivilegeType.READ_DATA, privilegeList));

    // root.t2 have read data privilege because root.**
    Assert.assertTrue(AuthUtils.checkPathPrivilege(path2, PrivilegeType.READ_DATA, privilegeList));
    Assert.assertFalse(
        AuthUtils.hasPrivilegeToRevoke(path2, PrivilegeType.READ_DATA, privilegeList));

    Assert.assertEquals(AuthUtils.getPrivileges(path, privilegeList).size(), 1);
    Assert.assertEquals(AuthUtils.getPrivileges(path, null).size(), 0);
    pathWithPri.grantPrivilege(PrivilegeType.WRITE_DATA, false);
    Assert.assertTrue(
        AuthUtils.getPrivileges(path, privilegeList).contains(PrivilegeType.WRITE_DATA));
  }

  @Test
  public void authUtilsTest_PathPrivilegeAddRemove() throws IllegalPathException, AuthException {
    List<PathPrivilege> privs = new ArrayList<>();
    PartialPath path1 = new PartialPath("root.t1");

    AuthUtils.addPrivilege(path1, PrivilegeType.READ_SCHEMA, privs, false);
    AuthUtils.addPrivilege(path1, PrivilegeType.READ_DATA, privs, false);
    AuthUtils.addPrivilege(path1, PrivilegeType.WRITE_SCHEMA, privs, true);

    Assert.assertEquals(privs.size(), 1);
    Assert.assertEquals(privs.get(0).getPrivilegeIntSet().size(), 3);
    Assert.assertEquals(privs.get(0).getGrantOpt().size(), 1);

    PartialPath path2 = new PartialPath("root.t2");
    AuthUtils.addPrivilege(path2, PrivilegeType.READ_SCHEMA, privs, false);

    Assert.assertEquals(privs.size(), 2);

    AuthUtils.removePrivilege(path2, PrivilegeType.READ_SCHEMA, privs);
    Assert.assertEquals(privs.size(), 1);

    // if we revoke privileges on root.**, privileges on root.t1 and root.t2 will also be removed.
    PartialPath rootPath = new PartialPath("root.**");
    AuthUtils.removePrivilege(rootPath, PrivilegeType.READ_DATA, privs);
    Assert.assertEquals(2, privs.get(0).getPrivilegeIntSet().size());
    Assert.assertFalse(privs.get(0).getPrivileges().contains(PrivilegeType.READ_DATA));

    AuthUtils.addPrivilege(path2, PrivilegeType.WRITE_SCHEMA, privs, true);
    AuthUtils.removePrivilege(rootPath, PrivilegeType.WRITE_SCHEMA, privs);
    Assert.assertEquals(1, privs.size());
    Assert.assertEquals(1, privs.get(0).getPrivilegeIntSet().size());
  }

  @Test
  public void authUtilsTest_PatternPathCheck() throws AuthException, IllegalPathException {
    AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1")));
    AuthUtils.validatePatternPath(new PartialPath(new String("root.data.**")));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.*a"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.*"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.a*.a"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.*.a"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.**.a"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.a*.*"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.*.*"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.**.*"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.a*.**"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.*.**"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1.**.**"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("*.data.t1.**.**"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("**.data.t1.**.**"))));
    Assert.assertThrows(
        AuthException.class,
        () -> AuthUtils.validatePatternPath(new PartialPath(new String("*a.data.t1.**.**"))));
  }
}
