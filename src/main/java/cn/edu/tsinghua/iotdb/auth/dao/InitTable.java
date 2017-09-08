package cn.edu.tsinghua.iotdb.auth.dao;

/**
 * @author liukun
 *
 */
public class InitTable {

	public static String createUserTableSql = "create table userTable("
			+ "id INT generated always  as identity(start with 1,increment by 1) not null primary key,"
			+ "userName VARCHAR(20) not null unique," + "password VARCHAR(20) not null,"
			+ "locked CHAR(1) check (locked='t' or locked='f')," + "validTime VARCHAR(20))";

	public static String createRoleTableSql = "create table roleTable("
			+ "id INT generated always as identity(start with 1, increment by 1) not null primary key,"
			+ "roleName VARCHAR(20) not null unique)";

	public static String createUserRoleRelTableSql = "create table userRoleRelTable("
			+ "id INT generated always as identity(start with 1, increment by 1) ," + "userId INT," + "roleId INT,"
			+ "constraint pk_userrolerel primary key (userId,roleId),"
			+ "foreign key (userId) references usertable(id)  on delete cascade,"
			+ "foreign key (roleId) references roletable(id) on delete cascade)";

	public static String creteUserPermissionTableSql = "create table userPermissionTable("
			+ "id INT generated always as identity(start with 1,increment by 1) ," + "userId INT not null,"
			+ "nodeName VARCHAR(20) not null," + "permissionId INT not null,"
			+ "constraint pk_userpermission primary key (userId,nodeName,permissionId),"
			+ "foreign key (userId) references usertable(id) on delete cascade)";

	public static String createRolePermissionTableSql = "create table rolePermissionTable("
			+ "id INT generated always as identity(start with 1, increment by 1)," + "roleId INT not null,"
			+ "nodeName VARCHAR(20) not null," + "permissionId INT not null,"
			+ "constraint pk_rolepermission primary key (roleId,nodeName,permissionId),"
			+ "foreign key (roleId) references roleTable(id) on delete cascade)";

	public static String insertIntoUserToTableSql = "insert into usertable (username,password) values('root','root')";
}
