package cn.edu.thu.tsfiledb.tool;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class CSVToTsfile {
	private static final int MAX_HELP_CONSOLE_WIDTH = 88;
	private static final String ISO8601_ARGS = "disableISO8601";
	private static boolean timeFormatInISO8601 = true;

	private static String host;
	private static String port;
	private static String username;
	private static String password;
	private static String filename;
	private static String timeformat;

	private static HashMap<String, String> hm = null;
	private static HashMap<String, ArrayList<Integer>> hm_timeseries; //storage device info
	private static ArrayList<String> headInfo;   //storage csv head info
	private static ArrayList<String> colInfo;	//storage csv  sensor info 

	private static Connection connection = null;
	private static Statement statement = null;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		if (args == null || args.length == 0) {
			System.out.println("usage: host  port  username  password  csv_filename timeformat");
			return;
		}
		host = args[0];
		port = args[1];
		username = args[2];
		password = args[3];
		filename = args[4];
		timeformat = args[5];
		//SetTimeFormat(timeformat);
		Class.forName("cn.edu.thu.tsfiledb.jdbc.TsfileDriver");
		connection = DriverManager.getConnection("jdbc:tsfile://" + host + ":" + port + "/", username, password);
		loadDataFromCSV();

		connection.close();

	}

	private static void SetTimeFormat(String tf) {
		if(tf.equals("ISO8601")) {
			
		}else if(tf.equals("timestamps")) {
			
		}else {
			
		}
	}

	public static void loadDataFromCSV() throws IOException {
		try {
			statement = connection.createStatement();
			BufferedReader br = new BufferedReader(new InputStreamReader
					(new FileInputStream(new File(filename))));
			String line = "";
			String[] str_headInfo = br.readLine().split(",");
			hm_timeseries = new HashMap<String, ArrayList<Integer>>();
			colInfo = new ArrayList<String>();
			headInfo = new ArrayList<String>();

			createType();  //create metadata info

			for (int i = 1; i < str_headInfo.length; i++) {
				if (hm.containsKey(str_headInfo[i]) == false) {
					System.out.println("data colum not exist!");
					System.exit(1);
				}
				headInfo.add(str_headInfo[i]);
				String deviceInfo = str_headInfo[i].substring(0, str_headInfo[i].lastIndexOf("."));
				if (hm_timeseries.containsKey(deviceInfo)) {
					hm_timeseries.get(deviceInfo).add(i - 1); //storage every device's sensor index info
					colInfo.add(str_headInfo[i].substring(str_headInfo[i].lastIndexOf(".") + 1));
				} else {
					hm_timeseries.put(deviceInfo, new ArrayList<Integer>());
					hm_timeseries.get(deviceInfo).add(i - 1); //storage every device's sensor index info
					colInfo.add(str_headInfo[i].substring(str_headInfo[i].lastIndexOf(".") + 1));
				}

			}
//			for (Entry<String, ArrayList<Integer>> entry : hm_timeseries.entrySet()) {
//				System.out.println(entry.getKey() + " " + entry.getValue());
//			}
//			System.out.println(colInfo);
			while ((line = br.readLine()) != null) {
				ArrayList<String> sqls = createInsertSQL(line);
				for (String str : sqls) {
					statement.execute(str);
				}
//				ResultSet rs = statement.executeQuery("select d1.s1 from root.fit");
//				while (rs.next()) {
//					System.out.println(rs.getString(0) + " " +  rs.getString(1));
//				}
			}
			br.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

	private static String typeIdentify(String key_timeseries) {
		return hm.get(key_timeseries);

	}

	private static void createType() {
		hm = new HashMap<String, String>();
		hm.put("root.fit.d1.s1", "INT32");
		hm.put("root.fit.d1.s2", "BYTE_ARRAY");
		hm.put("root.fit.d2.s1", "INT32");
		hm.put("root.fit.d2.s3", "INT32");
		hm.put("root.fit.p.s1", "INT32");
	}

	private static ArrayList<String> createInsertSQL(String line) {
		String[] words = line.split(",", 6);
		long timestamps = Long.valueOf(words[0]);
		
		ArrayList<String> sqls = new ArrayList<String>();
		Iterator<Map.Entry<String, ArrayList<Integer>>> it = hm_timeseries.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, ArrayList<Integer>> entry = it.next();
			String sql = "";
			ArrayList<Integer> colIndex = entry.getValue();
			sql = "insert into " + entry.getKey() + "(timestamp";
			int skipcount = 0;
			for (int j = 0; j < colIndex.size(); ++j) {

				if (words[entry.getValue().get(j) + 1].equals("")) {
					skipcount++;
					continue;
				}
				sql += (", " + colInfo.get(colIndex.get(j)));
			}
			if (skipcount == entry.getValue().size())
				continue;
			sql = sql + ") values(" + timestamps;
			for (int j = 0; j < colIndex.size(); ++j) {
				if (words[entry.getValue().get(j) + 1].equals(""))
					continue;
				if (typeIdentify(headInfo.get(colIndex.get(j))) == "BYTE_ARRAY") {
					sql = sql + ", \'" + words[colIndex.get(j) + 1] + "\'";
				} else {
					sql = sql + "," + words[colIndex.get(j) + 1];
				}
			}
			sql += ")";
			sqls.add(sql);
//			System.out.println(sql);
		}

		return sqls;
	}

}
