package cn.edu.thu.tsfiledb.auth;

import cn.edu.thu.tsfiledb.auth.dao.Authorizer;
import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.auth.model.User;

public class TestAu {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// 启动server的时候需要open db
		DBdao dBdao = new DBdao();
		dBdao.open();
		System.out.println("start server.....");
		// 操作数据库信息
		User user = new User("test", "test");
		
		try {
			Authorizer.createUser(user.getUserName(), user.getPassWord());
			Authorizer.deleteUser(user.getUserName());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 关闭server的时候需要 close db
		System.out.println("close the server...");
		dBdao.close();
	}

}
