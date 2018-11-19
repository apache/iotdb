package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.thrift.TSDataValue;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSQueryDataSet;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TSRowRecord;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_Status;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_StatusCode;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

public class UtilsTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParseURL() throws TsfileURLException {
		String userName = "test";
		String userPwd = "test";
		String host = "localhost";
		int port = 6667;
		Properties properties = new Properties();
		properties.setProperty(TsfileJDBCConfig.AUTH_USER, userName);
		properties.setProperty(TsfileJDBCConfig.AUTH_PASSWORD, userPwd);
		TsfileConnectionParams params = Utils.parseURL(String.format("jdbc:tsfile://%s:%s/", host, port), properties);
		assertEquals(params.getHost(), host);
		assertEquals(params.getPort(), port);
		assertEquals(params.getUsername(), userName);
		assertEquals(params.getPassword(), userPwd);
	}

	@Test
	public void testVerifySuccess() {
		try {
			Utils.verifySuccess(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
		} catch (Exception e) {
			fail();
		}
		
		try {
			Utils.verifySuccess(new TS_Status(TS_StatusCode.ERROR_STATUS));
		} catch (Exception e) {
			return;
		}
		fail();
	}

	@Test
	public void testConvertRowRecords() {
		final int DATA_TYPE_NUM = 6;
		Object[][] input = { 
				{ 
					100L, 
					"sensor1_boolean", TSDataType.BOOLEAN, false, 
					"sensor1_int32", TSDataType.INT32, 100, 
					"sensor1_int64", TSDataType.INT64, 9999999999L, 
					"sensor1_float", TSDataType.FLOAT, 1.23f,
					"sensor1_double", TSDataType.DOUBLE, 1004234.435d, 
					"sensor1_text", TSDataType.TEXT, "iotdb-jdbc", 
				}, 
				{ 
					200L, 
					"sensor2_boolean", TSDataType.BOOLEAN, true, 
					"sensor2_int32", TSDataType.INT32, null, 
					"sensor2_int64", TSDataType.INT64, -9999999999L, 
					"sensor2_float", TSDataType.FLOAT, null,
					"sensor2_double", TSDataType.DOUBLE, -1004234.435d, 
					"sensor2_text", TSDataType.TEXT, null, 
				}, 
				{ 
					300L, 
					"sensor3_boolean", TSDataType.BOOLEAN, null, 
					"sensor3_int32", TSDataType.INT32, -100, 
					"sensor3_int64", TSDataType.INT64, null, 
					"sensor3_float", TSDataType.FLOAT, -1.23f,
					"sensor3_double", TSDataType.DOUBLE, null, 
					"sensor3_text", TSDataType.TEXT, "jdbc-iotdb", 
				}, 		
		};
		TSQueryDataSet tsQueryDataSet = new TSQueryDataSet(new ArrayList<>());
		for(Object[] item: input) {
			TSRowRecord record = new TSRowRecord();
			record.setTimestamp((long)item[0]);
			List<String> keys = new ArrayList<>();
			List<TSDataValue> values = new ArrayList<>();
			for(int i = 0; i < DATA_TYPE_NUM;i++) {
				keys.add((String)item[3*i+1]);
				TSDataValue value = new TSDataValue(false);
				if(item[3*i+3] == null) {
					value.setIs_empty(true);
				} else {
					if(i == 0) {
						value.setBool_val((boolean)item[3*i+3]);
						value.setType(((TSDataType) item[3*i+2]).toString());
					} else if (i == 1) {
						value.setInt_val((int) item[3 * i + 3]);
						value.setType(((TSDataType) item[3*i+2]).toString());
					} else if (i == 2) {
						value.setLong_val((long) item[3 * i + 3]);
						value.setType(((TSDataType) item[3*i+2]).toString());
					} else if (i == 3) {
						value.setFloat_val((float) item[3 * i + 3]);
						value.setType(((TSDataType) item[3*i+2]).toString());
					} else if (i == 4) {
						value.setDouble_val((double) item[3 * i + 3]);
						value.setType(((TSDataType) item[3*i+2]).toString());
					} else {
						value.setBinary_val(ByteBuffer.wrap(((String) item[3*i+3]).getBytes()));
						value.setType(((TSDataType) item[3*i+2]).toString());
					}
				}
				values.add(value);
			}
			record.setKeys(keys);
			record.setValues(values);
			tsQueryDataSet.getRecords().add(record);
		}
		List<RowRecord> convertlist = Utils.convertRowRecords(tsQueryDataSet);
		int index = 0;
		for (RowRecord r : convertlist) {
			assertEquals(input[index][0], r.getTimestamp());
			LinkedHashMap<Path, TsPrimitiveType> fields = r.getFields();
			int j = 0;
			for (Path p : fields.keySet()) {
				String pString = p.getFullPath();
				assertEquals(input[index][3 * j + 1], pString);
//				System.out.println(String.format("%d--%d", index, j));
				if (j == 0) {
					if(input[index][3 * j + 3] == null){
						assertEquals(input[index][3 * j + 3], fields.get(p));
					} else {
						assertEquals(input[index][3 * j + 3], fields.get(p).getBoolean());
					}
				} else if (j == 1) {
					if(input[index][3 * j + 3] == null){
						assertEquals(input[index][3 * j + 3], fields.get(p));
					} else {
						assertEquals(input[index][3 * j + 3], fields.get(p).getInt());
					}
				} else if (j == 2) {
					if(input[index][3 * j + 3] == null){
						assertEquals(input[index][3 * j + 3], fields.get(p));
					} else {
						assertEquals(input[index][3 * j + 3], fields.get(p).getLong());
					}
				} else if (j == 3) {
					if(input[index][3 * j + 3] == null){
						assertEquals(input[index][3 * j + 3], fields.get(p));
					} else {
						assertEquals(input[index][3 * j + 3], fields.get(p).getFloat());
					}
				} else if (j == 4) {
					if(input[index][3 * j + 3] == null){
						assertEquals(input[index][3 * j + 3], fields.get(p));
					} else {
						assertEquals(input[index][3 * j + 3], fields.get(p).getDouble());
					}
				} else {
					if(input[index][3 * j + 3] == null){
						assertEquals(input[index][3 * j + 3], fields.get(p));
					} else {
						assertEquals(input[index][3 * j + 3], fields.get(p).getStringValue());
					}
				}
				j++;
			}
			index++;
		}
	}

}
