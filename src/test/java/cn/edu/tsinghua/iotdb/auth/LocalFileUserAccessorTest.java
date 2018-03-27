package cn.edu.tsinghua.iotdb.auth;

import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.User;
import cn.edu.tsinghua.iotdb.auth.user.LocalFileUserAccessor;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LocalFileUserAccessorTest {

    private File testFolder;
    private LocalFileUserAccessor accessor;

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.envSetUp();
        testFolder = new File("test/");
        testFolder.mkdirs();
        accessor = new LocalFileUserAccessor(testFolder.getPath());
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void test() throws IOException {
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

        // save
        for (User user : users) {
            try {
                accessor.saveUser(user);
            } catch (IOException e) {
                fail(e.getMessage());
            }
        }

        // load
        for (User user : users) {
            try {
                User loadedUser = accessor.loadUser(user.name);
                assertEquals(user, loadedUser);
            } catch (IOException e) {
                fail(e.getMessage());
            }
        }
        assertEquals(null, accessor.loadUser("not a user"));

        // list
        List<String> usernames = accessor.listAllUsers();
        usernames.sort(null);
        for(int i = 0; i < users.length; i++) {
            assertEquals(users[i].name, usernames.get(i));
        }

        // delete
        assertEquals(false, accessor.deleteUser("not a user"));
        assertEquals(true, accessor.deleteUser(users[users.length - 1].name));
        usernames = accessor.listAllUsers();
        assertEquals(users.length - 1, usernames.size());
        usernames.sort(null);
        for(int i = 0; i < users.length - 1; i++) {
            assertEquals(users[i].name, usernames.get(i));
        }
        User nullUser = accessor.loadUser(users[users.length - 1].name);
        assertEquals(null, nullUser);
    }
}
