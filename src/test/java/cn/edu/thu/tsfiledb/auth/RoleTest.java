package cn.edu.thu.tsfiledb.auth;

import static org.junit.Assert.*;

import java.sql.Statement;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.auth.dao.RoleDao;
import cn.edu.thu.tsfiledb.auth.model.Role;

public class RoleTest {

	private Statement statement = null;
	private DBdao dbdao = null;
	private RoleDao roleDao = null;
	private Role role = null;

	private String roleName = "role";

	@Before
	public void setUp() throws Exception {
		dbdao = new DBdao();
		dbdao.open();
		statement = dbdao.getStatement();
		roleDao = new RoleDao();
	}

	@After
	public void tearDown() throws Exception {
		dbdao.close();
	}

	@Test
	public void test() {
		role = roleDao.getRole(statement, roleName);
		if (role != null) {
			System.out.println("Delete the original role");
			roleDao.deleteRole(statement, roleName);
		}
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

}
