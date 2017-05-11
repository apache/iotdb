package cn.edu.thu.tsfiledb.auth;

import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.auth.model.User;

public class AuthDemon {

	public static void main(String[] args) {
		
		// open db
		DBdao dBdao = new DBdao();
		dBdao.open();
		User user = new User("demo", "demo");
		// operation
		try {
			Authorizer.createUser(user.getUserName(), user.getPassWord());
			Authorizer.deleteUser(user.getUserName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// close db
		dBdao.close();
	}

}
