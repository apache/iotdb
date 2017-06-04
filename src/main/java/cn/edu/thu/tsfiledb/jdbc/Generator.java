package cn.edu.thu.tsfiledb.jdbc;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.sun.management.OperatingSystemMXBean;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;

@SuppressWarnings("restriction")
public class Generator {
	private static final Logger LOGGER = LoggerFactory.getLogger(Generator.class);

	private String deviceName;
	private OperatingSystemMXBean osmb;

	private final String createTimeSeriesTemplate = "create timeseries root.%s.laptop.%s with datatype=%s,encoding=%s";
	private final String insertDataTemplate = "insert into root.%s.laptop.%s values(%s,%s)";
	private final String queryDataTemplate = "select laptop.* from root.%s where time < %s";
	private final String setFileLevelTemplate = "set storage group to root.%s.laptop";
	
	private final long SLEEP_INTERVAL = 5000;
	
//	private final String JDBC_SERVER_URL = "jdbc:tsfile://127.0.0.1:6667/x";
	private final String JDBC_SERVER_URL = "jdbc:tsfile://192.168.130.15:6667/x";
	

	public Generator(String deviceName) {
		this.deviceName = deviceName;
		this.osmb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

	}

	public void start() throws ClassNotFoundException, SQLException {
		Class.forName("com.corp.tsfile.jdbc.TsfileDriver");
		createTimeSeriesMetadata();
		while (true) {
			try {
				insertData();
				Thread.sleep(SLEEP_INTERVAL);
			} catch (SQLException e) {
				LOGGER.error("tsfile-jdbc Generator: insertData failed because ", e);
			} catch (InterruptedException e) {
				LOGGER.error("tsfile-jdbc Generator: thread sleep failed because ", e);
			}
		}		
	}

	public void test() throws SQLException, ClassNotFoundException {
		Class.forName("com.corp.tsfile.jdbc.TsfileDriver");
		Connection connection = DriverManager.getConnection(JDBC_SERVER_URL, "root", "root");
		Statement statement = connection.createStatement();
		ResultSet res = statement.executeQuery(String.format(queryDataTemplate,  deviceName, getCurretnTimestamp()));
		Client.output(res, true);
		connection.close();
	}

	private void createTimeSeriesMetadata() throws SQLException {
		List<String> sqls = new ArrayList<>();
		sqls.add(String.format(createTimeSeriesTemplate, deviceName, "cpu", TSDataType.FLOAT.toString(),
				TSEncoding.RLE.toString()));
		sqls.add(String.format(createTimeSeriesTemplate, deviceName, "memory", TSDataType.INT64.toString(),
				TSEncoding.RLE.toString()));
		sqls.add(String.format(setFileLevelTemplate, deviceName));
		executeSQL(sqls);
	}

	private void insertData() throws SQLException {
		List<String> sqls = new ArrayList<>();
		String timestamp = String.valueOf(getCurretnTimestamp());
		sqls.add(String.format(insertDataTemplate, deviceName, "cpu", timestamp, getCpuRatio()));
		sqls.add(String.format(insertDataTemplate, deviceName, "memory", timestamp, getFreePhysicalMemorySize()));
		executeSQL(sqls);
	}

	private void executeSQL(List<String> sqls) throws SQLException {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(JDBC_SERVER_URL, "root", "root");
			Statement statement = connection.createStatement();
			for (String sql : sqls) {
				try {
					statement.execute(sql);
				} catch (Exception e) {
					LOGGER.error("tsfile-jdbc Generator: execute {} failed!", sql, e);
					continue;
				}
				LOGGER.info("tsfile-jdbc Generator: execute {} successfully!", sql);
			}
		} catch (SQLException e) {
			LOGGER.error("tsfile-jdbc Generator: fail to execute {} because ", sqls, e);
		} finally {
			if (connection != null) {
				connection.close();
				connection = null;
			}
		}
	}

	public String getCpuRatio() {
		double cpuRatio =  osmb.getSystemCpuLoad() * 100;
		if (cpuRatio < 0 || cpuRatio > 100) {
			return "0";
		}
		return String.format("%.2f", cpuRatio);
	}

	private String getFreePhysicalMemorySize() {
		long sizeFree = osmb.getFreePhysicalMemorySize();
		return sizeFree > 0 ? String.valueOf(sizeFree) : "0";
	}

	private String getCurretnTimestamp() {
		return String.valueOf(System.currentTimeMillis());
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
		Generator demo = new Generator("caogaofei_pc");
		// insert data
		demo.start();
		
		// query data
//		demo.test();
	}

}
