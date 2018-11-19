package cn.edu.tsinghua.iotdb.auth;

import cn.edu.tsinghua.iotdb.auth.Role.LocalFileRoleAccessor;
import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LocalFIleRoleAccessorTest {
    private File testFolder;
    private LocalFileRoleAccessor accessor;

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.envSetUp();
        testFolder = new File("test/");
        testFolder.mkdirs();
        accessor = new LocalFileRoleAccessor(testFolder.getPath());
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(testFolder);
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void test() throws IOException {
        Role[] roles = new Role[5];
        for (int i = 0; i < roles.length; i++) {
            roles[i] = new Role("role" + i);
            for(int j = 0; j <= i; j++) {
                PathPrivilege pathPrivilege = new PathPrivilege("root.a.b.c" + j);
                pathPrivilege.privileges.add(j);
                roles[i].privilegeList.add(pathPrivilege);
            }
        }

        // save
        for(Role role : roles) {
            accessor.saveRole(role);
        }

        // load
        for(Role role : roles) {
            Role loadedRole = accessor.loadRole(role.name);
            assertEquals(role, loadedRole);
        }
        assertEquals(null, accessor.loadRole("not a role"));

        // delete
        assertEquals(true, accessor.deleteRole(roles[roles.length - 1].name));
        assertEquals(false, accessor.deleteRole(roles[roles.length - 1].name));
        assertEquals(null, accessor.loadRole(roles[roles.length - 1].name));

        // list
        List<String> roleNames = accessor.listAllRoles();
        roleNames.sort(null);
        for(int i = 0; i < roleNames.size(); i++) {
            assertEquals(roles[i].name, roleNames.get(i));
        }
    }
}
