package cn.edu.tsinghua.iotdb.auth;

import static org.junit.Assert.assertEquals;

import java.sql.Statement;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.auth.dao.DBDao;
import cn.edu.tsinghua.iotdb.auth.dao.RoleDao;
import cn.edu.tsinghua.iotdb.auth.model.Role;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class RoleTest {

	private Statement statement = null;
	private DBDao dbdao = null;
	private RoleDao roleDao = null;
	private Role role = null;
	private String roleName = "role";

	@Before
	public void setUp() throws Exception {
		dbdao = new DBDao();
		dbdao.open();
		EnvironmentUtils.envSetUp();
		statement = DBDao.getStatement();
		roleDao = new RoleDao();
	}

	@After
	public void tearDown() throws Exception {
		dbdao.close();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void createAndDeleteTest() {
		role = roleDao.getRole(statement, roleName);
		if (role != null) {
			System.out.println("Delete the original role");
			roleDao.deleteRole(statement, roleName);
		}
		assertEquals(null, roleDao.getRole(statement, roleName));
		// create role
		role = new Role(roleName);
		roleDao.createRole(statement, role);
		Role getRole = roleDao.getRole(statement, roleName);
		assertEquals(roleName, getRole.getRoleName());
		// delete role
		roleDao.deleteRole(statement, roleName);
		role = roleDao.getRole(statement, roleName);
		assertEquals(null, role);
	}

	@Test
	public void getRolesTest() {
		Role role1 = new Role("role1");
		Role role2 = new Role("role2");
		ArrayList<Role> arrayList = new ArrayList<>();
		arrayList.add(role1);
		arrayList.add(role2);
		if (roleDao.getRole(statement, role1.getRoleName()) == null) {
			roleDao.createRole(statement, role1);
		}
		if (roleDao.getRole(statement, role2.getRoleName()) == null) {
			roleDao.createRole(statement, role2);
		}
		ArrayList<Role> list = (ArrayList<Role>) roleDao.getRoles(statement);
		ArrayList<String> getRoleNames = new ArrayList<>();
		for (Role role : list) {
			getRoleNames.add(role.getRoleName());
		}
		ArrayList<String> remove = new ArrayList<>();
		remove.add(role1.getRoleName());
		remove.add(role2.getRoleName());
		getRoleNames.removeAll(remove);
		assertEquals(0, getRoleNames.size());
		roleDao.deleteRole(statement, role1.getRoleName());
		roleDao.deleteRole(statement, role2.getRoleName());
	}
	
	@Test
	public void updateRoleTest(){
		// insert one role
		Role role1 = new Role("role1");
		Role role2 = new Role("role2");
		if (roleDao.getRole(statement, role1.getRoleName()) == null) {
			roleDao.createRole(statement, role1);
		}
		// update the role
		int state = roleDao.updateRole(statement, role1.getRoleName(), role2.getRoleName());
		assertEquals(1, state);
		// check the role
		Role role = roleDao.getRole(statement, role2.getRoleName());
		assertEquals(true, role!=null);
		assertEquals(role2.getRoleName(), role.getRoleName());
	}
}
