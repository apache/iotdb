package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthorizationTest {

    private IoTDB deamon;

    private boolean testFlag = TestUtils.testFlag;

    @Before
    public void setUp() throws Exception {
        if (testFlag) {
            EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (testFlag) {
            deamon.stop();
            Thread.sleep(2000);
            EnvironmentUtils.cleanEnv();
        }
    }

    @Test
    public void illegalGrantRevokeUserTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        // grant a non-existing user
        boolean caught = false;
        try {
            adminStmt.execute("GRANT USER nulluser PRIVILEGES 'SET_STORAGE_GROUP' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant a non-existing privilege
        caught = false;
        try {
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'NOT_A_PRIVILEGE' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // duplicate grant
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        caught = false;
        try {
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant on a illegal path
        caught = false;
        try {
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant admin
        caught = false;
        try {
            adminStmt.execute("GRANT USER root PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // no privilege to grant
        caught = false;
        try {
            userStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke a non-existing privilege
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        caught = false;
        try {
            adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'CREATE_USER' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke a non-existing user
        caught = false;
        try {
            adminStmt.execute("REVOKE USER tempuser1 PRIVILEGES 'CREATE_USER' on root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke on a illegal path
        caught = false;
        try {
            adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // revoke admin
        caught = false;
        try {
            adminStmt.execute("REVOKE USER root PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // no privilege to revoke
        caught = false;
        try {
            userStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant privilege to grant
        caught = false;
        try {
            userStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'GRANT_USER_PRIVILEGE' on root");
        userStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root");

        // grant privilege to revoke
        caught = false;
        try {
            userStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'REVOKE_USER_PRIVILEGE' on root");
        userStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root");
    }

    @Test
    public void createDeleteTimeSeriesTest() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        // grant and revoke the user the privilege to create time series
        boolean caught = false;
        try {
            userStmt.execute("SET STORAGE GROUP TO root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'SET_STORAGE_GROUP' ON root.a");
        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");

        caught = false;
        try {
            // no privilege to create this one
            userStmt.execute("SET STORAGE GROUP TO root.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            // privilege already exists
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'SET_STORAGE_GROUP' ON root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'SET_STORAGE_GROUP' ON root.a");
        caught = false;
        try {
            // no privilege to create this one any more
            userStmt.execute("SET STORAGE GROUP TO root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // the user cannot delete the timeseries now
        caught = false;
        try {
            // no privilege to create this one any more
            userStmt.execute("DELETE TIMESERIES root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // the user can delete the timeseries now
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a");
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.b");
        userStmt.execute("DELETE TIMESERIES root.a.b");

        // revoke the privilege to delete time series
        adminStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        adminStmt.execute("SET STORAGE GROUP TO root.b");
        adminStmt.execute("CREATE TIMESERIES root.b.a WITH DATATYPE=INT32,ENCODING=PLAIN");
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.a");
        userStmt.execute("DELETE TIMESERIES root.b.a");
        caught = false;
        try {
            // no privilege to create this one any more
            userStmt.execute("DELETE TIMESERIES root.a.b");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminCon.close();
        userCon.close();
    }

    @Test
    public void insertQueryTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'SET_STORAGE_GROUP' ON root.a");
        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");

        // grant privilege to insert
        boolean caught = false;
        try {
            userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'INSERT_TIMESERIES' on root.a");
        userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");

        // revoke privilege to insert
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'INSERT_TIMESERIES' on root.a");
        caught = false;
        try {
            userStmt.execute("INSERT INTO root.a(timestamp, b) VALUES (1,100)");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        // grant privilege to query
        caught = false;
        try {
            userStmt.execute("SELECT * from root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'READ_TIMESERIES' on root.a");
        userStmt.execute("SELECT * from root.a");
        userStmt.getResultSet().close();

        // revoke privilege to query
        adminStmt.execute("REVOKE USER tempuser PRIVILEGES 'READ_TIMESERIES' on root.a");
        caught = false;
        try {
            userStmt.execute("SELECT * from root.a");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminCon.close();
        userCon.close();
    }

    @Test
    public void rolePrivilegeTest() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        boolean caught = false;
        try {
            userStmt.execute("CREATE ROLE admin");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);
        adminStmt.execute("CREATE ROLE admin");
        adminStmt.execute("GRANT ROLE admin PRIVILEGES 'SET_STORAGE_GROUP','DELETE_TIMESERIES','READ_TIMESERIES','INSERT_TIMESERIES' on root");
        adminStmt.execute("GRANT admin TO tempuser");

        userStmt.execute("SET STORAGE GROUP TO root.a");
        userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("CREATE TIMESERIES root.a.c WITH DATATYPE=INT32,ENCODING=PLAIN");
        userStmt.execute("INSERT INTO root.a(timestamp,b,c) VALUES (1,100,1000)");
        userStmt.execute("DELETE FROM root.a.b WHERE TIME <= 1000000000");
        userStmt.execute("SELECT * FROM root");
        userStmt.getResultSet().close();

        adminStmt.execute("REVOKE ROLE admin PRIVILEGES 'DELETE_TIMESERIES' on root");
        caught = false;
        try {
            userStmt.execute("DELETE FROM root.* WHERE TIME <= 1000000000");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminStmt.execute("GRANT USER tempuser PRIVILEGES 'READ_TIMESERIES' on root");
        adminStmt.execute("REVOKE admin FROM tempuser");
        userStmt.execute("SELECT * FROM root");
        userStmt.getResultSet().close();
        caught = false;
        try {
            userStmt.execute("CREATE TIMESERIES root.a.b WITH DATATYPE=INT32,ENCODING=PLAIN");
        } catch (SQLException e) {
            caught = true;
        }
        assertTrue(caught);

        adminCon.close();
        userCon.close();
    }

    @Test
    @Ignore
    public void authPerformanceTest() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER tempuser temppw");
        adminStmt.execute("SET STORAGE GROUP TO root.a");
        int privilegeCnt = 500;
        for (int i = 0; i < privilegeCnt; i++) {
            adminStmt.execute("CREATE TIMESERIES root.a.b" + i + " WITH DATATYPE=INT32,ENCODING=PLAIN");
            adminStmt.execute("GRANT USER tempuser PRIVILEGES 'INSERT_TIMESERIES' ON root.a.b" + i);
        }

        Connection userCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "tempuser", "temppw");
        Statement userStmt = userCon.createStatement();

        int insertCnt = 2000000;
        int batchSize = 5000;
        long time;

        time = System.currentTimeMillis();
        for (int i = 0; i < insertCnt; ) {
            for (int j = 0; j < batchSize; j++)
                userStmt.addBatch("INSERT INTO root.a(timestamp, b" + (privilegeCnt - 1) + ") VALUES (" + (i++ + 1) + ", 100)");
            userStmt.executeBatch();
            userStmt.clearBatch();
        }
        System.out.println("User inserted " + insertCnt + " data points used " + (System.currentTimeMillis() - time) + " ms with " + privilegeCnt + " privileges");

        time = System.currentTimeMillis();
        for (int i = 0; i < insertCnt; ) {
            for (int j = 0; j < batchSize; j++)
                adminStmt.addBatch("INSERT INTO root.a(timestamp, b0) VALUES (" + (i++ + 1 + insertCnt) + ", 100)");
            adminStmt.executeBatch();
            adminStmt.clearBatch();
        }
        System.out.println("admin inserted " + insertCnt + " data points used " + (System.currentTimeMillis() - time) + " ms with " + privilegeCnt + " privileges");

        adminCon.close();
        userCon.close();
    }

    @Test
    public void testListUser() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        try {
            adminStmt.execute("LIST USER");
        } catch (SQLException e) {
            assertEquals("Users are : [ \n" +
                    "root\n" +
                    "]", e.getMessage());
        }

        for (int i = 0; i < 10; i++) {
            adminStmt.execute("CREATE USER user" + i + " password" + i);
        }
        try {
            adminStmt.execute("LIST USER");
        } catch (SQLException e) {
            assertEquals("Users are : [ \n" +
                    "root\n" +
                    "user0\n" +
                    "user1\n" +
                    "user2\n" +
                    "user3\n" +
                    "user4\n" +
                    "user5\n" +
                    "user6\n" +
                    "user7\n" +
                    "user8\n" +
                    "user9\n" +
                    "]", e.getMessage());
        }

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0)
                adminStmt.execute("DROP USER user" + i);
        }
        try {
            adminStmt.execute("LIST USER");
        } catch (SQLException e) {
            assertEquals("Users are : [ \n" +
                    "root\n" +
                    "user1\n" +
                    "user3\n" +
                    "user5\n" +
                    "user7\n" +
                    "user9\n" +
                    "]", e.getMessage());
        }

        adminCon.close();
    }

    @Test
    public void testListRole() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        try {
            adminStmt.execute("LIST ROLE");
        } catch (SQLException e) {
            assertEquals("Roles are : [ \n" +
                    "]", e.getMessage());
        }

        for (int i = 0; i < 10; i++) {
            adminStmt.execute("CREATE ROLE role" + i);
        }
        try {
            adminStmt.execute("LIST ROLE");
        } catch (SQLException e) {
            assertEquals("Roles are : [ \n" +
                    "role0\n" +
                    "role1\n" +
                    "role2\n" +
                    "role3\n" +
                    "role4\n" +
                    "role5\n" +
                    "role6\n" +
                    "role7\n" +
                    "role8\n" +
                    "role9\n" +
                    "]", e.getMessage());
        }

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0)
                adminStmt.execute("DROP ROLE role" + i);
        }
        try {
            adminStmt.execute("LIST ROLE");
        } catch (SQLException e) {
            assertEquals("Roles are : [ \n" +
                    "role1\n" +
                    "role3\n" +
                    "role5\n" +
                    "role7\n" +
                    "role9\n" +
                    "]", e.getMessage());
        }

        adminCon.close();
    }

    @Test
    public void testListUserPrivileges() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER user1 password1");
        adminStmt.execute("GRANT USER user1 PRIVILEGES 'READ_TIMESERIES' ON root.a.b");
        adminStmt.execute("CREATE ROLE role1");
        adminStmt.execute("GRANT ROLE role1 PRIVILEGES 'READ_TIMESERIES','INSERT_TIMESERIES','DELETE_TIMESERIES' ON root.a.b.c");
        adminStmt.execute("GRANT ROLE role1 PRIVILEGES 'READ_TIMESERIES','INSERT_TIMESERIES','DELETE_TIMESERIES' ON root.d.b.c");
        adminStmt.execute("GRANT role1 TO user1");

        try {
            adminStmt.execute("LIST USER PRIVILEGES  user1");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "From itself : {\n" +
                    "root.a.b : READ_TIMESERIES\n" +
                    "}\n" +
                    "From role role1 : {\n" +
                    "root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES\n" +
                    "root.d.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES\n" +
                    "}\n" +
                    "]", e.getMessage());
        }

        try {
            adminStmt.execute("LIST PRIVILEGES USER user1 ON root.a.b.c");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "From itself : {\n" +
                    "root.a.b : READ_TIMESERIES\n" +
                    "}\n" +
                    "From role role1 : {\n" +
                    "root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES\n" +
                    "}\n" +
                    "]", e.getMessage());
        }

        adminStmt.execute("REVOKE role1 from user1");
        try {
            adminStmt.execute("LIST USER PRIVILEGES  user1");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "From itself : {\n" +
                    "root.a.b : READ_TIMESERIES\n" +
                    "}\n" +
                    "]", e.getMessage());
        }

        try {
            adminStmt.execute("LIST PRIVILEGES USER user1 ON root.a.b.c");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "From itself : {\n" +
                    "root.a.b : READ_TIMESERIES\n" +
                    "}\n" +
                    "]", e.getMessage());
        }

        adminCon.close();
    }

    @Test
    public void testListRolePrivileges() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE ROLE role1");
        adminStmt.execute("GRANT ROLE role1 PRIVILEGES 'READ_TIMESERIES','INSERT_TIMESERIES','DELETE_TIMESERIES' ON root.a.b.c");
        adminStmt.execute("GRANT ROLE role1 PRIVILEGES 'READ_TIMESERIES','INSERT_TIMESERIES','DELETE_TIMESERIES' ON root.d.b.c");

        try {
            adminStmt.execute("LIST ROLE PRIVILEGES role1");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES\n" +
                    "root.d.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES\n" +
                    "]", e.getMessage());
        }

        try {
            adminStmt.execute("LIST PRIVILEGES ROLE role1 ON root.a.b.c");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "root.a.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES\n" +
                    "]", e.getMessage());
        }

        adminStmt.execute("REVOKE ROLE role1 PRIVILEGES 'INSERT_TIMESERIES','DELETE_TIMESERIES' ON root.a.b.c");

        try {
            adminStmt.execute("LIST ROLE PRIVILEGES role1");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "root.a.b.c : READ_TIMESERIES\n" +
                    "root.d.b.c : INSERT_TIMESERIES READ_TIMESERIES DELETE_TIMESERIES\n" +
                    "]", e.getMessage());
        }

        try {
            adminStmt.execute("LIST PRIVILEGES ROLE role1 ON root.a.b.c");
        } catch (SQLException e) {
            assertEquals("Privileges are : [ \n" +
                    "root.a.b.c : READ_TIMESERIES\n" +
                    "]", e.getMessage());
        }

        adminCon.close();
    }

    @Test
    public void testListUserRoles() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE USER chenduxiu orange");

        adminStmt.execute("CREATE ROLE xijing");
        adminStmt.execute("CREATE ROLE dalao");
        adminStmt.execute("CREATE ROLE shenshi");
        adminStmt.execute("CREATE ROLE zhazha");
        adminStmt.execute("CREATE ROLE hakase");

        adminStmt.execute("GRANT xijing TO chenduxiu");
        adminStmt.execute("GRANT dalao TO chenduxiu");
        adminStmt.execute("GRANT shenshi TO chenduxiu");
        adminStmt.execute("GRANT zhazha TO chenduxiu");
        adminStmt.execute("GRANT hakase TO chenduxiu");

        try {
            adminStmt.execute("LIST ALL ROLE OF USER chenduxiu");
        } catch (SQLException e) {
            assertEquals("Roles are : [ \n" +
                    "xijing\n" +
                    "dalao\n" +
                    "shenshi\n" +
                    "zhazha\n" +
                    "hakase\n" +
                    "]", e.getMessage());
        }

        adminStmt.execute("REVOKE dalao FROM chenduxiu");
        adminStmt.execute("REVOKE hakase FROM chenduxiu");

        try {
            adminStmt.execute("LIST ALL ROLE OF USER chenduxiu");
        } catch (SQLException e) {
            assertEquals("Roles are : [ \n" +
                    "xijing\n" +
                    "shenshi\n" +
                    "zhazha\n" +
                    "]", e.getMessage());
        }

        adminCon.close();
    }

    @Test
    public void testListRoleUsers() throws SQLException, ClassNotFoundException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection adminCon = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
        Statement adminStmt = adminCon.createStatement();

        adminStmt.execute("CREATE ROLE dalao");
        adminStmt.execute("CREATE ROLE zhazha");

        String[] members = {
                "HighFly", "SunComparison", "Persistence", "GoodWoods", "HealthHonor", "GoldLuck", "DoubleLight", "Eastwards",
                "ScentEffusion", "Smart", "East", "DailySecurity", "Moon", "RayBud", "RiverSky"
        };

        for (int i = 0; i < members.length - 1; i++) {
            adminStmt.execute("CREATE USER " + members[i] + " 666666");
            adminStmt.execute("GRANT dalao TO  " + members[i]);
        }
        adminStmt.execute("CREATE USER RiverSky 2333333");
        adminStmt.execute("GRANT zhazha TO RiverSky");

        try {
            adminStmt.execute("LIST ALL USER OF ROLE dalao");
        } catch (SQLException e) {
            assertEquals("Users are : [ \n" +
                    "DailySecurity\n" +
                    "DoubleLight\n" +
                    "East\n" +
                    "Eastwards\n" +
                    "GoldLuck\n" +
                    "GoodWoods\n" +
                    "HealthHonor\n" +
                    "HighFly\n" +
                    "Moon\n" +
                    "Persistence\n" +
                    "RayBud\n" +
                    "ScentEffusion\n" +
                    "Smart\n" +
                    "SunComparison\n" +
                    "]", e.getMessage());
        }

        try {
            adminStmt.execute("LIST ALL USER OF ROLE zhazha");
        } catch (SQLException e) {
            assertEquals("Users are : [ \n" +
                    "RiverSky\n" +
                    "]", e.getMessage());
        }

        adminStmt.execute("REVOKE zhazha from RiverSky");
        try {
            adminStmt.execute("LIST ALL USER OF ROLE zhazha");
        } catch (SQLException e) {
            assertEquals("Users are : [ \n" +
                    "]", e.getMessage());
        }

        adminCon.close();
    }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            AuthorizationTest test = new AuthorizationTest();
            test.setUp();
            test.authPerformanceTest();
            test.tearDown();
        }
    }
}
