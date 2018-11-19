package cn.edu.tsinghua.iotdb.postback.sender;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderConfig;
import cn.edu.tsinghua.iotdb.postback.conf.PostBackSenderDescriptor;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.service.TestUtils;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

/**
 * The test is to run a complete postback function
 * Before you run the test, make sure receiver has been cleaned up and inited.
 */
public class IoTDBSingleClientPostBackTest {

	private TsfileDBConfig conf = TsfileDBDescriptor.getInstance().getConfig();
	
	private String serverIpTest = "192.168.130.17";
	private PostBackSenderConfig config= PostBackSenderDescriptor.getInstance().getConfig();;
	private Set<String> dataSender = new HashSet<>();
	private Set<String> dataReceiver = new HashSet<>();
	private boolean success = true;
	FileSenderImpl fileSenderImpl = FileSenderImpl.getInstance();

	private IoTDB deamon;
	
	public void setConfig() {
		config.uuidPath = config.dataDirectory + "postback" + File.separator + "uuid.txt";
		config.lastFileInfo = config.dataDirectory + "postback" + File.separator + "lastLocalFileList.txt";

		String[] snapshots = new String[config.iotdbBufferwriteDirectory.length];
		for (int i = 0; i < config.iotdbBufferwriteDirectory.length; i++) {
			config.iotdbBufferwriteDirectory[i] = new File(config.iotdbBufferwriteDirectory[i]).getAbsolutePath();
			if (!config.iotdbBufferwriteDirectory[i].endsWith(File.separator)) {
				config.iotdbBufferwriteDirectory[i] = config.iotdbBufferwriteDirectory[i] + File.separator;
			}
			snapshots[i] = config.iotdbBufferwriteDirectory[i] + "postback" + File.separator + "dataSnapshot"
					+ File.separator;
		}
		config.snapshotPaths = snapshots;
		config.serverIp = serverIpTest;
		fileSenderImpl.setConfig(config);
	}
	
	private String[] sqls1 = new String[] { 
			"SET STORAGE GROUP TO root.vehicle",
			"SET STORAGE GROUP TO root.test",
			"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
			"CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
			"CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
			"CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
			"CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
			"CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
			"CREATE TIMESERIES root.test.d1.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
			"insert into root.vehicle.d0(timestamp,s0) values(10,100)",
			"insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
			"insert into root.vehicle.d0(timestamp,s1) values(19,'103')",
			"insert into root.vehicle.d1(timestamp,s2) values(11,104.0)",
			"insert into root.vehicle.d1(timestamp,s2,s3) values(15,105.0,true)",
			"insert into root.vehicle.d1(timestamp,s3) values(17,false)",
			"insert into root.vehicle.d0(timestamp,s0) values(20,1000)",
			"insert into root.vehicle.d0(timestamp,s0,s1) values(22,1001,'1002')",
			"insert into root.vehicle.d0(timestamp,s1) values(29,'1003')",
			"insert into root.vehicle.d1(timestamp,s2) values(21,1004.0)",
			"insert into root.vehicle.d1(timestamp,s2,s3) values(25,1005.0,true)",
			"insert into root.vehicle.d1(timestamp,s3) values(27,true)",
			"insert into root.test.d0(timestamp,s0) values(10,106)",
			"insert into root.test.d0(timestamp,s0,s1) values(14,107,'108')",
			"insert into root.test.d0(timestamp,s1) values(16,'109')",
			"insert into root.test.d1.g0(timestamp,s0) values(1,110)",
			"insert into root.test.d0(timestamp,s0) values(30,1006)",
			"insert into root.test.d0(timestamp,s0,s1) values(34,1007,'1008')",
			"insert into root.test.d0(timestamp,s1) values(36,'1090')",
			"insert into root.test.d1.g0(timestamp,s0) values(10,1100)",
			"merge",
			"flush",
			};

	private String[] sqls2 = new String[] { 
			"insert into root.vehicle.d0(timestamp,s0) values(6,120)",
			"insert into root.vehicle.d0(timestamp,s0,s1) values(38,121,'122')",
			"insert into root.vehicle.d0(timestamp,s1) values(9,'123')",
			"insert into root.vehicle.d0(timestamp,s0) values(16,128)",
			"insert into root.vehicle.d0(timestamp,s0,s1) values(18,189,'198')",
			"insert into root.vehicle.d0(timestamp,s1) values(99,'1234')",
			"insert into root.vehicle.d1(timestamp,s2) values(14,1024.0)",
			"insert into root.vehicle.d1(timestamp,s2,s3) values(29,1205.0,true)",
			"insert into root.vehicle.d1(timestamp,s3) values(33,true)",
			"insert into root.test.d0(timestamp,s0) values(15,126)",
			"insert into root.test.d0(timestamp,s0,s1) values(8,127,'128')",
			"insert into root.test.d0(timestamp,s1) values(20,'129')",
			"insert into root.test.d1.g0(timestamp,s0) values(14,430)",
			"insert into root.test.d0(timestamp,s0) values(150,426)",
			"insert into root.test.d0(timestamp,s0,s1) values(80,427,'528')",
			"insert into root.test.d0(timestamp,s1) values(2,'1209')",
			"insert into root.test.d1.g0(timestamp,s0) values(4,330)",
			"merge",
			"flush",
			};

	private String[] sqls3 = new String[] { 
			"SET STORAGE GROUP TO root.iotdb", 
			"SET STORAGE GROUP TO root.flush",
			"CREATE TIMESERIES root.iotdb.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
			"CREATE TIMESERIES root.iotdb.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
			"CREATE TIMESERIES root.iotdb.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
			"CREATE TIMESERIES root.iotdb.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
			"CREATE TIMESERIES root.flush.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
			"CREATE TIMESERIES root.flush.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
			"CREATE TIMESERIES root.flush.d1.g0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
			"insert into root.iotdb.d0(timestamp,s0) values(3,100)",
			"insert into root.iotdb.d0(timestamp,s0,s1) values(22,101,'102')",
			"insert into root.iotdb.d0(timestamp,s1) values(24,'103')",
			"insert into root.iotdb.d1(timestamp,s2) values(21,104.0)",
			"insert into root.iotdb.d1(timestamp,s2,s3) values(25,105.0,true)",
			"insert into root.iotdb.d1(timestamp,s3) values(27,false)",
			"insert into root.iotdb.d0(timestamp,s0) values(30,1000)",
			"insert into root.iotdb.d0(timestamp,s0,s1) values(202,101,'102')",
			"insert into root.iotdb.d0(timestamp,s1) values(44,'103')",
			"insert into root.iotdb.d1(timestamp,s2) values(1,404.0)",
			"insert into root.iotdb.d1(timestamp,s2,s3) values(250,10.0,true)",
			"insert into root.iotdb.d1(timestamp,s3) values(207,false)",
			"insert into root.flush.d0(timestamp,s0) values(20,106)",
			"insert into root.flush.d0(timestamp,s0,s1) values(14,107,'108')",
			"insert into root.flush.d1.g0(timestamp,s0) values(1,110)",
			"insert into root.flush.d0(timestamp,s0) values(200,1006)",
			"insert into root.flush.d0(timestamp,s0,s1) values(1004,1007,'1080')",
			"insert into root.flush.d1.g0(timestamp,s0) values(1000,910)",
			"insert into root.vehicle.d0(timestamp,s0) values(209,130)",
			"insert into root.vehicle.d0(timestamp,s0,s1) values(206,131,'132')",
			"insert into root.vehicle.d0(timestamp,s1) values(70,'33')",
			"insert into root.vehicle.d1(timestamp,s2) values(204,14.0)",
			"insert into root.vehicle.d1(timestamp,s2,s3) values(29,135.0,false)",
			"insert into root.vehicle.d1(timestamp,s3) values(14,false)",
			"insert into root.test.d0(timestamp,s0) values(19,136)",
			"insert into root.test.d0(timestamp,s0,s1) values(7,137,'138')",
			"insert into root.test.d0(timestamp,s1) values(30,'139')",
			"insert into root.test.d1.g0(timestamp,s0) values(4,150)",
			"insert into root.test.d0(timestamp,s0) values(1900,1316)",
			"insert into root.test.d0(timestamp,s0,s1) values(700,1307,'1038')",
			"insert into root.test.d0(timestamp,s1) values(3000,'1309')",
			"insert into root.test.d1.g0(timestamp,s0) values(400,1050)",
			"merge",
			"flush",
			};

	private boolean testFlag = TestUtils.testFlag;

	public void setUp() throws Exception {
		if (testFlag) {
			EnvironmentUtils.closeStatMonitor();
            EnvironmentUtils.closeMemControl();
            deamon = IoTDB.getInstance();
            deamon.active();
            EnvironmentUtils.envSetUp();
            conf.overflowFileSizeThreshold = 0;
		}
		setConfig();
	}

	public void tearDown() throws Exception {
		if (testFlag) {
			deamon.stop();
			Thread.sleep(2000);
			EnvironmentUtils.cleanEnv();
		}
		if(success)
			System.out.println("Test succeed!");
		else
			System.out.println("Test failed!");
	}
	
	public void testPostback() {
		if (testFlag) {
			// the first time to postback
			System.out.println("It's the first time to post back!");
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
					Statement statement = connection.createStatement();
					for (String sql : sqls1) {
						statement.execute(sql);
					}
					statement.close();
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			
			fileSenderImpl.postback();

			// Compare data of sender and receiver
			dataSender.clear();
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
					Statement statement = connection.createStatement();
					boolean hasResultSet = statement.execute("select * from root.vehicle");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
						}
					}
					hasResultSet = statement.execute("select * from root.test");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
						}
					}
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			
			dataReceiver.clear();
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.17:6667/", "root", "root");
					Statement statement = connection.createStatement();
					boolean hasResultSet = statement.execute("select * from root.vehicle");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
						}
					}
					
					hasResultSet = statement.execute("select * from root.test");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
						}
					}
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			System.out.println(dataSender.size());
			System.out.println(dataReceiver.size());
			System.out.println(dataSender);
			System.out.println(dataReceiver);
			if(!(dataSender.size()==dataReceiver.size() && dataSender.containsAll(dataReceiver))){
				success = false;
				return;
			}
			
			// the second time to postback
			System.out.println("It's the second time to post back!");
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
					Statement statement = connection.createStatement();
					for (String sql : sqls2) {
						statement.execute(sql);
					}
					statement.close();
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			
			fileSenderImpl.postback();
			
			// Compare data of sender and receiver
			dataSender.clear();
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
					Statement statement = connection.createStatement();
					boolean hasResultSet = statement.execute("select * from root.vehicle");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
						}
					}
					hasResultSet = statement.execute("select * from root.test");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
						}
					}
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			
			dataReceiver.clear();
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.17:6667/", "root", "root");
					Statement statement = connection.createStatement();
					boolean hasResultSet = statement.execute("select * from root.vehicle");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
						}
					}
					hasResultSet = statement.execute("select * from root.test");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
						}
					}
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			System.out.println(dataSender.size());
			System.out.println(dataReceiver.size());
			System.out.println(dataSender);
			System.out.println(dataReceiver);
			if(!(dataSender.size()==dataReceiver.size() && dataSender.containsAll(dataReceiver))){
				success = false;
				return;
			}
			
			// the third time to postback
			System.out.println("It's the third time to post back!");
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
					Statement statement = connection.createStatement();
					for (String sql : sqls3) {
						statement.execute(sql);
					}
					statement.close();
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}

			fileSenderImpl.postback();
	
			// Compare data of sender and receiver
			dataSender.clear();
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
					Statement statement = connection.createStatement();
					boolean hasResultSet = statement.execute("select * from root.vehicle");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
						}
					}
					hasResultSet = statement.execute("select * from root.test");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
						}
					}
					hasResultSet = statement.execute("select * from root.flush");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.flush.d0.s0") + res.getString("root.flush.d0.s1")
									+ res.getString("root.flush.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.flush.d0.s0") + res.getString("root.flush.d0.s1")
									+ res.getString("root.flush.d1.g0.s0"));
						}
					}
					hasResultSet = statement.execute("select * from root.iotdb");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataSender.add(res.getString("Time") + res.getString("root.iotdb.d0.s0") + res.getString("root.iotdb.d0.s1")
							+ res.getString("root.iotdb.d1.s2") + res.getString("root.iotdb.d1.s3"));
							System.out.println(res.getString("Time") + res.getString("root.iotdb.d0.s0") + res.getString("root.iotdb.d0.s1")
							+ res.getString("root.iotdb.d1.s2") + res.getString("root.iotdb.d1.s3"));
						}
					}
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
	
			dataReceiver.clear();
			try {
				Thread.sleep(2000);
				Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
				Connection connection = null;
				try {
					connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.17:6667/", "root", "root");
					Statement statement = connection.createStatement();
					boolean hasResultSet = statement.execute("select * from root.vehicle");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.vehicle.d0.s0")
									+ res.getString("root.vehicle.d0.s1") + res.getString("root.vehicle.d1.s2")
									+ res.getString("root.vehicle.d1.s3"));
						}
					}
					hasResultSet = statement.execute("select * from root.test");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.test.d0.s0")
									+ res.getString("root.test.d0.s1") + res.getString("root.test.d1.g0.s0"));
						}
					}
					hasResultSet = statement.execute("select * from root.flush");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.flush.d0.s0") + res.getString("root.flush.d0.s1")
									+ res.getString("root.flush.d1.g0.s0"));
							System.out.println(
									res.getString("Time") + " | " + res.getString("root.flush.d0.s0") + res.getString("root.flush.d0.s1")
									+ res.getString("root.flush.d1.g0.s0"));
						}
					}
					hasResultSet = statement.execute("select * from root.iotdb");
					if (hasResultSet) {
						ResultSet res = statement.getResultSet();
						while (res.next()) {
							dataReceiver.add(res.getString("Time") + res.getString("root.iotdb.d0.s0") + res.getString("root.iotdb.d0.s1")
							+ res.getString("root.iotdb.d1.s2") + res.getString("root.iotdb.d1.s3"));
							System.out.println(res.getString("Time") + res.getString("root.iotdb.d0.s0") + res.getString("root.iotdb.d0.s1")
							+ res.getString("root.iotdb.d1.s2") + res.getString("root.iotdb.d1.s3"));
						}
					}
					statement.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (connection != null) {
						connection.close();
					}
				}
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
			System.out.println(dataSender.size());
			System.out.println(dataReceiver.size());
			System.out.println(dataSender);
			System.out.println(dataReceiver);
			if(!(dataSender.size()==dataReceiver.size() && dataSender.containsAll(dataReceiver))){
				success = false;
				return;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		IoTDBSingleClientPostBackTest singleClientPostBackTest = new IoTDBSingleClientPostBackTest();
		singleClientPostBackTest.setUp();
		singleClientPostBackTest.testPostback();
		singleClientPostBackTest.tearDown();
		System.exit(0);
	}
}
