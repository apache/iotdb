package org.apache.iotdb.db.auth.role;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.role.LocalFileRoleManager;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LocalFileRoleManagerTest {

  private File testFolder;
  private LocalFileRoleManager manager;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    testFolder = new File(TestConstant.BASE_OUTPUT_PATH.concat("test"));
    testFolder.mkdirs();
    manager = new LocalFileRoleManager(testFolder.getPath());
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testFolder);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws AuthException, IllegalPathException {
    Role[] roles = new Role[4];
    for (int i = 0; i < roles.length; i++) {
      roles[i] = new Role("role" + i);
      for (int j = 0; j <= i; j++) {
        PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.a.b.c" + j));
        pathPrivilege.grantPrivilege(PrivilegeType.values()[j], false);
        roles[i].getPathPrivilegeList().add(pathPrivilege);
        roles[i].getSysPrivilege().add(PrivilegeType.values()[j + 4]);
      }
    }

    // create
    Role role = manager.getEntry(roles[0].getName());
    assertNull(role);
    for (Role role1 : roles) {
      assertTrue(manager.createEntry(role1.getName()));
    }
    for (Role role1 : roles) {
      role = manager.getEntry(role1.getName());
      assertEquals(role1.getName(), role.getName());
    }

    assertFalse(manager.createEntry(roles[0].getName()));

    // delete
    assertFalse(manager.deleteEntry("not a role"));
    assertTrue(manager.deleteEntry(roles[roles.length - 1].getName()));
    assertNull(manager.getEntry(roles[roles.length - 1].getName()));
    assertFalse(manager.deleteEntry(roles[roles.length - 1].getName()));

    // grant privilege
    role = manager.getEntry(roles[0].getName());
    PartialPath path = new PartialPath("root.a.b.c");
    assertFalse(role.hasPrivilegeToRevoke(path, PrivilegeType.READ_DATA));
    manager.grantPrivilegeToEntry(
        role.getName(), new PrivilegeUnion(path, PrivilegeType.READ_DATA));
    manager.grantPrivilegeToEntry(
        role.getName(), new PrivilegeUnion(path, PrivilegeType.WRITE_DATA));

    // grant again will success
    manager.grantPrivilegeToEntry(
        role.getName(), new PrivilegeUnion(path, PrivilegeType.WRITE_DATA));
    role = manager.getEntry(roles[0].getName());
    assertTrue(role.hasPrivilegeToRevoke(path, PrivilegeType.READ_DATA));
    manager.grantPrivilegeToEntry(role.getName(), new PrivilegeUnion(PrivilegeType.MAINTAIN));
    manager.grantPrivilegeToEntry(
        role.getName(), new PrivilegeUnion(PrivilegeType.MANAGE_ROLE, true));
    boolean caught = false;
    try {
      manager.grantPrivilegeToEntry("not a role", new PrivilegeUnion(PrivilegeType.MANAGE_ROLE));
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // revoke privilege
    role = manager.getEntry(roles[0].getName());
    assertTrue(
        manager.revokePrivilegeFromEntry(
            role.getName(), new PrivilegeUnion(PrivilegeType.MAINTAIN)));
    assertFalse(
        manager.revokePrivilegeFromEntry(
            role.getName(), new PrivilegeUnion(PrivilegeType.MANAGE_USER)));
    assertFalse(
        manager.revokePrivilegeFromEntry(
            role.getName(),
            new PrivilegeUnion(new PartialPath("root.test"), PrivilegeType.WRITE_SCHEMA)));
    assertEquals(1, manager.getEntry(role.getName()).getSysPriGrantOpt().size());
    caught = false;
    try {
      manager.revokePrivilegeFromEntry("not a role", new PrivilegeUnion(PrivilegeType.MAINTAIN));
    } catch (AuthException e) {
      caught = true;
    }
    assertTrue(caught);

    // list roles
    List<String> rolenames = manager.listAllEntries();
    rolenames.sort(null);
    for (int i = 0; i < roles.length - 1; i++) {
      assertEquals(roles[i].getName(), rolenames.get(i));
    }
  }
}
