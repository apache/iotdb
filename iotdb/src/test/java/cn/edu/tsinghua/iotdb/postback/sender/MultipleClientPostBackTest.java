package cn.edu.tsinghua.iotdb.postback.sender;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;

public class MultipleClientPostBackTest {

	Map<String,ArrayList<String>> timeseriesList = new HashMap();
	Map<String,ArrayList<String>> timeseriesList1 = new HashMap();
	private Set<String> dataSender = new HashSet<>();
	private Set<String> dataReceiver = new HashSet<>();
	
	public void testPostback() throws IOException {
		
		timeseriesList1.put("root.vehicle_history1",new ArrayList<String>());
        timeseriesList1.put("root.vehicle_alarm1",new ArrayList<String>());
        timeseriesList1.put("root.vehicle_temp1",new ArrayList<String>());
        timeseriesList1.put("root.range_event1",new ArrayList<String>());
        timeseriesList.put("root.vehicle_history",new ArrayList<String>());
        timeseriesList.put("root.vehicle_alarm",new ArrayList<String>());
        timeseriesList.put("root.vehicle_temp",new ArrayList<String>());
        timeseriesList.put("root.range_event",new ArrayList<String>());
		
        File file = new File("CreateTimeseries1.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
            String timeseries = line.split(" ")[2];
            for(String storageGroup:timeseriesList.keySet()) {
            	if(timeseries.startsWith(storageGroup + ".")) {
            		String timesery = timeseries.substring((storageGroup + ".").length());
            		timeseriesList.get(storageGroup).add(timesery);
            		break;
            	}
            }
        }
        
        file = new File("CreateTimeseries2.txt");
        reader = new BufferedReader(new FileReader(file));
        while ((line = reader.readLine()) != null) {
            String timeseries = line.split(" ")[2];
            for(String storageGroup:timeseriesList1.keySet()) {
            	if(timeseries.startsWith(storageGroup + ".")) {
            		String timesery = timeseries.substring((storageGroup + ".").length());
            		timeseriesList1.get(storageGroup).add(timesery);
            		break;
            	}
            }
        }
        
		// Compare data of sender and receiver
//        for(String storageGroup:timeseriesList.keySet()) {
//        	String sqlFormat = "select %s from %s where time < 1518090019515";
//        	System.out.println(storageGroup + ":");
//        	int count=0;
//        	int count1 =0 ;
//        	int count2 = 0;
//        	for(String timesery:timeseriesList.get(storageGroup)) {
//        		count++;
//            	count1=0;
//            	count2=0;
//        		dataSender.clear();
//            	dataReceiver.clear();
//        		try {
//					Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
//					Connection connection = null;
//					Connection connection1 = null;
//					try {
//						connection = DriverManager.getConnection("jdbc:tsfile://166.111.7.249:6667/", "root", "root");
//						connection1 = DriverManager.getConnection("jdbc:tsfile://166.111.7.250:6667/", "root", "root");
//						Statement statement = connection.createStatement();
//						Statement statement1 = connection1.createStatement();
//						String SQL = String.format(sqlFormat, timesery, storageGroup);
//						System.out.println(SQL);
//						boolean hasResultSet = statement.execute(SQL);
//						boolean hasResultSet1 = statement1.execute(SQL);
//						if (hasResultSet) {
//							ResultSet res = statement.getResultSet();
//							while (res.next()) {
//								count1++;
//								dataSender.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
//							}
//						}
//						if (hasResultSet1) {
//							ResultSet res = statement1.getResultSet();
//							while (res.next()) {
//								count2++;
//								dataReceiver.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
//							}
//						}
//						assert((dataSender.size()==dataReceiver.size()) && dataSender.containsAll(dataReceiver));
//						statement.close();
//						statement1.close();
//					} catch (Exception e) {
//						e.printStackTrace();
//					} finally {
//						if (connection != null) {
//							connection.close();
//						}
//						if (connection1 != null) {
//							connection1.close();
//						}
//					}
//				} catch (ClassNotFoundException | SQLException e) {
//					fail(e.getMessage());
//				}
//            	System.out.println(count1);
//            	System.out.println(count2);
//            	if(count > 1)
//            		break;
//        	}
//        }
		for(String storageGroup:timeseriesList.keySet()) {
			String sqlFormat = "select %s from %s";
			System.out.println(storageGroup + ":");
			int count=0;
			int count1 =0 ;
			int count2 = 0;
			for(String timesery:timeseriesList.get(storageGroup)) {
				count++;
				count1=0;
				count2=0;
				dataSender.clear();
				dataReceiver.clear();
				try {
					Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
					Connection connection = null;
					Connection connection1 = null;
					try {
						connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.14:6667/", "root", "root");
						connection1 = DriverManager.getConnection("jdbc:tsfile://192.168.130.16:6667/", "root", "root");
						Statement statement = connection.createStatement();
						Statement statement1 = connection1.createStatement();
						String SQL = String.format(sqlFormat, timesery, storageGroup);
						boolean hasResultSet = statement.execute(SQL);
						boolean hasResultSet1 = statement1.execute(SQL);
						if (hasResultSet) {
							ResultSet res = statement.getResultSet();
							while (res.next()) {
								count1++;
								dataSender.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
							}
						}
						if (hasResultSet1) {
							ResultSet res = statement1.getResultSet();
							while (res.next()) {
								count2++;
								dataReceiver.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
							}
						}
						assert((dataSender.size()==dataReceiver.size()) && dataSender.containsAll(dataReceiver));
						statement.close();
						statement1.close();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (connection != null) {
							connection.close();
						}
						if (connection1 != null) {
							connection1.close();
						}
					}
				} catch (ClassNotFoundException | SQLException e) {
					fail(e.getMessage());
				}
				if(count > 20)
					break;
				System.out.println(count1);
				System.out.println(count2);
			}
		}

        for(String storageGroup:timeseriesList1.keySet()) {
        	String sqlFormat = "select %s from %s";
        	System.out.println(storageGroup + ":");
        	int count=0;
        	int count1 =0 ;
        	int count2 = 0; 
        	for(String timesery:timeseriesList1.get(storageGroup)) {
            	count++; 
            	count1=0;
            	count2=0;
        		dataSender.clear();
            	dataReceiver.clear();
        		try {
					Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
					Connection connection = null;
					Connection connection1 = null;
					try {
						connection = DriverManager.getConnection("jdbc:tsfile://192.168.130.15:6667/", "root", "root");
						connection1 = DriverManager.getConnection("jdbc:tsfile://192.168.130.16:6667/", "root", "root");
						Statement statement = connection.createStatement();
						Statement statement1 = connection1.createStatement();
						String SQL = String.format(sqlFormat, timesery, storageGroup);
						boolean hasResultSet = statement.execute(SQL);
						boolean hasResultSet1 = statement1.execute(SQL);
						if (hasResultSet) {
							ResultSet res = statement.getResultSet();
							while (res.next()) {
								count1++;
								dataSender.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
							}
						}
						if (hasResultSet1) {
							ResultSet res = statement1.getResultSet();
							while (res.next()) {
								count2++;
								dataReceiver.add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
							}
						}
						assert((dataSender.size()==dataReceiver.size()) && dataSender.containsAll(dataReceiver));
						statement.close();
						statement1.close();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (connection != null) {
							connection.close();
						}
						if (connection1 != null) {
							connection1.close();
						}
					}
				} catch (ClassNotFoundException | SQLException e) {
					fail(e.getMessage());
				}
            	if(count > 20)
            		break;
            	System.out.println(count1);
            	System.out.println(count2);
        	}
        }
	}
	
	public static void main(String[] args) throws IOException {
		MultipleClientPostBackTest multipleClientPostBackTest = new MultipleClientPostBackTest();
		multipleClientPostBackTest.testPostback();
	}
}
