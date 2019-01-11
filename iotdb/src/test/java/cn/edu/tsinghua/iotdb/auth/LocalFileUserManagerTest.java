package cn.edu.tsinghua.iotdb.auth;

import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.User;
import cn.edu.tsinghua.iotdb.auth.user.LocalFileUserManager;
import cn.edu.tsinghua.iotdb.conf.IoTDBConstant;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.AuthUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocalFileUserManagerTest {

    private File testFolder;
    private LocalFileUserManager manager;

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.envSetUp();
        testFolder = new File("test/");
        testFolder.mkdirs();
        manager = new LocalFileUserManager(testFolder.getPath());
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void test() throws AuthException {
        User[] users = new User[5];
        for (int i = 0; i < users.length; i++) {
            users[i] = new User("user" + i, "password" + i);
            for(int j = 0; j <= i; j++) {
                PathPrivilege pathPrivilege = new PathPrivilege("root.a.b.c" + j);
                pathPrivilege.privileges.add(j);
                users[i].privilegeList.add(pathPrivilege);
                users[i].roleList.add("role" + j);
            }
        }

        // create
        User user = manager.getUser(users[0].name);
        assertEquals(null, user);
        for (User user1 : users) assertEquals(true, manager.createUser(user1.name, user1.password));
        for (User user1 : users) {
            user = manager.getUser(user1.name);
            assertEquals(user1.name, user.name);
            assertEquals(AuthUtils.encryptPassword(user1.password), user.password);
        }

        assertEquals(false, manager.createUser(users[0].name, users[0].password));
        boolean caught = false;
        try {
            manager.createUser("too", "short");
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);
        caught = false;
        try {
            manager.createUser("short", "too");
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);

        // delete
        assertEquals(false, manager.deleteUser("not a user"));
        assertEquals(true, manager.deleteUser(users[users.length-1].name));
        assertEquals(null, manager.getUser(users[users.length-1].name));
        assertEquals(false, manager.deleteUser(users[users.length-1].name));

        // grant privilege
        user = manager.getUser(users[0].name);
        String path = "root.a.b.c";
        int privilegeId = 0;
        assertEquals(false, user.hasPrivilege(path, privilegeId));
        assertEquals(true, manager.grantPrivilegeToUser(user.name, path, privilegeId));
        assertEquals(true, manager.grantPrivilegeToUser(user.name, path, privilegeId + 1));
        assertEquals(false, manager.grantPrivilegeToUser(user.name, path, privilegeId));
        user = manager.getUser(users[0].name);
        assertEquals(true, user.hasPrivilege(path, privilegeId));
        caught = false;
        try {
            manager.grantPrivilegeToUser("not a user", path, privilegeId);
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);
        caught = false;
        try {
            manager.grantPrivilegeToUser(user.name, path, -1);
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);

        // revoke privilege
        user = manager.getUser(users[0].name);
        assertEquals(true, manager.revokePrivilegeFromUser(user.name, path, privilegeId));
        assertEquals(false, manager.revokePrivilegeFromUser(user.name, path, privilegeId));
        caught = false;
        try {
            manager.revokePrivilegeFromUser("not a user", path, privilegeId);
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);
        caught = false;
        try {
            manager.revokePrivilegeFromUser(user.name, path, -1);
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);

        // update password
        String newPassword = "newPassword";
        String illegalPW = "new";
        assertEquals(true, manager.updateUserPassword(user.name, newPassword));
        assertEquals(false, manager.updateUserPassword(user.name, illegalPW));
        user = manager.getUser(user.name);
        assertEquals(AuthUtils.encryptPassword(newPassword), user.password);
        caught = false;
        try {
            manager.updateUserPassword("not a user", newPassword);
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);

        // grant role
        String roleName = "newrole";
        assertEquals(true, manager.grantRoleToUser(roleName, user.name));
        assertEquals(false, manager.grantRoleToUser(roleName, user.name));
        user = manager.getUser(user.name);
        assertEquals(true, user.hasRole(roleName));
        caught = false;
        try {
            manager.grantRoleToUser("not a user", roleName);
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);

        // revoke role
        assertEquals(true, manager.revokeRoleFromUser(roleName, user.name));
        assertEquals(false, manager.revokeRoleFromUser(roleName, user.name));
        user = manager.getUser(user.name);
        assertEquals(false, user.hasRole(roleName));
        caught = false;
        try {
            manager.revokeRoleFromUser("not a user", roleName);
        } catch (AuthException e) {
            caught = true;
        }
        assertEquals(true, caught);

        // list users
        List<String> usernames = manager.listAllUsers();
        usernames.sort(null);
        assertEquals(IoTDBConstant.ADMIN_NAME, usernames.get(0));
        for(int i = 0; i < users.length - 1; i++) {
            assertEquals(users[i].name, usernames.get(i + 1));
        }
    }
}
