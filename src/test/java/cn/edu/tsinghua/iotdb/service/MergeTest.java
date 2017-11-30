package cn.edu.tsinghua.iotdb.service;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;

import static org.junit.Assert.fail;

public class MergeTest {
	private final String FOLDER_HEADER = "src/test/resources";
	private static final String TIMESTAMP_STR = "Time";
	private final String d3s1 = "root.laptop.d3.s1";
	private final String d3s2 = "root.laptop.d3.s2";
	private final String d2s2 = "root.laptop.d2.s2";

	private String count(String path) {
		return String.format("count(%s)", path);
	}

	private String[] sqls = new String[] { "SET STORAGE GROUP TO root.laptop",
			"CREATE TIMESERIES root.laptop.d3.s1 WITH DATATYPE=INT32, ENCODING=RLE",
			"CREATE TIMESERIES root.laptop.d3.s2 WITH DATATYPE=INT32, ENCODING=RLE",
			"CREATE TIMESERIES root.laptop.d2.s2 WITH DATATYPE=INT32, ENCODING=RLE",
			"insert into root.laptop.d3(timestamp,s1) values(10,1)",
			"insert into root.laptop.d2(timestamp,s2) values(10,1)",
			"insert into root.laptop.d3(timestamp,s2) values(1,1)", "flush", "merge" };

	private String overflowDataDirPre;
	private String fileNodeDirPre;
	private String bufferWriteDirPre;
	private String metadataDirPre;
	private String derbyHomePre;

	private Daemon deamon;

	private boolean testFlag = false;

	@Before
	public void setUp() throws Exception {
		if (testFlag) {
			TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
			overflowDataDirPre = config.overflowDataDir;
			fileNodeDirPre = config.fileNodeDir;
			bufferWriteDirPre = config.bufferWriteDir;
			metadataDirPre = config.metadataDir;
			derbyHomePre = config.derbyHome;

			config.overflowDataDir = FOLDER_HEADER + "/data/overflow";
			config.fileNodeDir = FOLDER_HEADER + "/data/digest";
			config.bufferWriteDir = FOLDER_HEADER + "/data/delta";
			config.metadataDir = FOLDER_HEADER + "/data/metadata";
			config.derbyHome = FOLDER_HEADER + "/data/derby";
			deamon = new Daemon();
			deamon.active();
		}
	}

	@After
	public void tearDown() throws Exception {
		if (testFlag) {
			deamon.stop();
			Thread.sleep(5000);

			TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
			FileUtils.deleteDirectory(new File(config.overflowDataDir));
			FileUtils.deleteDirectory(new File(config.fileNodeDir));
			FileUtils.deleteDirectory(new File(config.bufferWriteDir));
			FileUtils.deleteDirectory(new File(config.metadataDir));
			FileUtils.deleteDirectory(new File(config.derbyHome));
			FileUtils.deleteDirectory(new File(FOLDER_HEADER + "/data"));

			config.overflowDataDir = overflowDataDirPre;
			config.fileNodeDir = fileNodeDirPre;
			config.bufferWriteDir = bufferWriteDirPre;
			config.metadataDir = metadataDirPre;
			config.derbyHome = derbyHomePre;
		}
	}

	private void insertSQL() throws ClassNotFoundException, SQLException {
		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
			Statement statement = connection.createStatement();
			for (String sql : sqls) {
				statement.execute(sql);
			}
			statement.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	@Test
	public void test() {
		if (testFlag) {
			try {
				Thread.sleep(5000);
				insertSQL();
				// Thread.sleep(5000);
				selectAllSQLTest();

				Connection connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
				connection.close();
			} catch (ClassNotFoundException | SQLException | InterruptedException e) {
				fail(e.getMessage());
			}
		}
	}

	private void selectAllSQLTest() throws ClassNotFoundException, SQLException {
		String[] retArray = new String[] { "1,null,1,null", "10,1,null,null", };

		Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
			Statement statement = connection.createStatement();
			boolean hasResultSet = statement.execute("select * from root.laptop");
			if (hasResultSet) {
				ResultSet resultSet = statement.getResultSet();
				int cnt = 0;
				while (resultSet.next()) {
					String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d3s1) + ","
							+ resultSet.getString(d3s2) + "," + resultSet.getString(d2s2);
					System.out.println(ans);
					cnt++;
				}
				// Assert.assertEquals(17, cnt);
			}
			statement.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
}
