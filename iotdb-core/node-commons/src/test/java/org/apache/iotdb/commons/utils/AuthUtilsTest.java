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
import java.util.Vector;

public class AuthUtilsTest {
  @Test
  public void authUtilsTest_ParameterCheck() throws AuthException, IllegalPathException {
    AuthUtils auth;
    Vector<String> nameOrPassword = new Vector<>();
    nameOrPassword.add(new String("he"));
    nameOrPassword.add(
        new String(
            "qwertyuiopasdfghjklzxcvbnm123456789999999asdfgh"
                + "jkzxcvbnmqwertyuioasdfghjklzxcvbnm"));
    nameOrPassword.add(new String("he  llo"));
    nameOrPassword.add(new String("hel^d"));
    nameOrPassword.add(new String("he\\llo"));
    nameOrPassword.add(new String("he*llo"));
    nameOrPassword.add(new String("he*$llo"));
    for (String item : nameOrPassword) {
      Assert.assertThrows(AuthException.class, () -> AuthUtils.validateNameOrPassword(item));
    }
    PartialPath path1 = new PartialPath(new String("data.t1"));
    PartialPath path2 = new PartialPath(new String("root.t1"));
    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePath(path1));
    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePrivilege(-1));
    // give a wrong path
    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePrivilege(path1, -1));
    // give a path but a wrong privilege id
    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePrivilege(path2, 5));

    Assert.assertThrows(AuthException.class, () -> AuthUtils.validatePrivilege(null, 3));
    AuthUtils.validatePrivilege(path2, PrivilegeType.WRITE_SCHEMA.ordinal());
    AuthUtils.validatePrivilege(null, PrivilegeType.MANAGE_ROLE.ordinal());
  }

  @Test
  public void authUtilsTest_PrivilegeGrantRevokeCheck() throws IllegalPathException {
    PartialPath path = new PartialPath(new String("root.t1"));
    PathPrivilege pathWithPri = new PathPrivilege(path);
    pathWithPri.grantPrivilege(PrivilegeType.READ_SCHEMA.ordinal(), false);
    pathWithPri.grantPrivilege(PrivilegeType.READ_DATA.ordinal(), false);

    PartialPath path2 = new PartialPath(new String("root.t2"));
    PathPrivilege pathWithPri2 = new PathPrivilege(path2);
    pathWithPri2.grantPrivilege(PrivilegeType.WRITE_SCHEMA.ordinal(), false);

    PartialPath path3 = new PartialPath(new String("root.**"));
    PathPrivilege pathWithPri3 = new PathPrivilege(path3);
    pathWithPri3.grantPrivilege(PrivilegeType.READ_DATA.ordinal(), false);

    /** root.t1 : read schema, read data root.t2 : write schema root.** : read data */
    // Privilege list is empty.
    Assert.assertFalse(
        AuthUtils.checkPathPrivilege(path2, PrivilegeType.READ_SCHEMA.ordinal(), null));

    List<PathPrivilege> privilegeList = new ArrayList<>();
    privilegeList.add(pathWithPri);
    privilegeList.add(pathWithPri2);
    privilegeList.add(pathWithPri3);
    Assert.assertFalse(
        AuthUtils.checkPathPrivilege(path2, PrivilegeType.READ_SCHEMA.ordinal(), privilegeList));
    Assert.assertTrue(
        AuthUtils.checkPathPrivilege(path, PrivilegeType.READ_SCHEMA.ordinal(), privilegeList));

    pathWithPri.revokePrivilege(PrivilegeType.READ_SCHEMA.ordinal());
    /** root.t1 : read data root.t2 : write schema root.** : read data */
    Assert.assertFalse(
        AuthUtils.checkPathPrivilege(path, PrivilegeType.READ_SCHEMA.ordinal(), privilegeList));
    Assert.assertTrue(
        AuthUtils.checkPathPrivilege(path, PrivilegeType.READ_DATA.ordinal(), privilegeList));

    // root.t2 have read data privilege because root.**
    Assert.assertTrue(
        AuthUtils.checkPathPrivilege(path2, PrivilegeType.READ_DATA.ordinal(), privilegeList));
    Assert.assertFalse(
        AuthUtils.hasPrivilege(path2, PrivilegeType.READ_DATA.ordinal(), privilegeList));

    Assert.assertEquals(AuthUtils.getPrivileges(path, privilegeList).size(), 1);
    Assert.assertEquals(AuthUtils.getPrivileges(path, null).size(), 0);
    pathWithPri.grantPrivilege(PrivilegeType.WRITE_DATA.ordinal(), false);
    Assert.assertTrue(
        AuthUtils.getPrivileges(path, privilegeList).contains(PrivilegeType.WRITE_DATA.ordinal()));
  }

  @Test
  public void authUtilsTest_PathPrivilegeAddRemove() throws IllegalPathException, AuthException {
    List<PathPrivilege> privs = new ArrayList<>();
    PartialPath path1 = new PartialPath("root.t1");

    AuthUtils.addPrivilege(path1, PrivilegeType.READ_SCHEMA.ordinal(), privs, false);
    AuthUtils.addPrivilege(path1, PrivilegeType.READ_DATA.ordinal(), privs, false);
    AuthUtils.addPrivilege(path1, PrivilegeType.WRITE_SCHEMA.ordinal(), privs, true);

    Assert.assertEquals(privs.size(), 1);
    Assert.assertEquals(privs.get(0).getPrivileges().size(), 3);
    Assert.assertEquals(privs.get(0).getGrantOpt().size(), 1);

    PartialPath path2 = new PartialPath("root.t2");
    AuthUtils.addPrivilege(path2, PrivilegeType.READ_SCHEMA.ordinal(), privs, false);

    Assert.assertEquals(privs.size(), 2);

    AuthUtils.removePrivilege(path2, PrivilegeType.READ_SCHEMA.ordinal(), privs);
    Assert.assertEquals(privs.size(), 1);

    AuthUtils.removePrivilege(path1, PrivilegeType.READ_DATA.ordinal(), privs);
    Assert.assertEquals(privs.get(0).getPrivileges().size(), 2);
    Assert.assertFalse(privs.get(0).getPrivileges().contains(PrivilegeType.READ_DATA.ordinal()));
  }

  @Test
  public void authUtilsTest_PatternPathCheck() throws AuthException, IllegalPathException {
    AuthUtils.validatePatternPath(new PartialPath(new String("root.data.t1")));
    AuthUtils.validatePatternPath(new PartialPath(new String("root.data.**")));
    AuthUtils.validatePatternPath(new PartialPath(new String("root.data.*a")));
    AuthUtils.validatePatternPath(new PartialPath(new String("root.data.*")));
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
