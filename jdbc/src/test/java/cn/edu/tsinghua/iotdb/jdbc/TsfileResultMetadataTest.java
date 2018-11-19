package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsfileResultMetadataTest {
	private TsfileResultMetadata metadata;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetColumnCount() throws SQLException {
		metadata = new TsfileResultMetadata(null, null, null);
		boolean flag = false;
		try {
			metadata.getColumnCount();
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		List<String> columnInfoList = new ArrayList<>();
		flag = false;
		try {
			metadata = new TsfileResultMetadata(columnInfoList, null, null);
			metadata.getColumnCount();
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		columnInfoList.add("root.a.b.c");
		assertEquals(metadata.getColumnCount(), 1);
	}


	@Test
	public void testGetColumnName() throws SQLException {
		metadata = new TsfileResultMetadata(null, null, null);
		boolean flag = false;
		try {
			metadata.getColumnName(1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		List<String> columnInfoList = new ArrayList<>();
		metadata = new TsfileResultMetadata(columnInfoList, null, null);
		flag = false;
		try {
			metadata.getColumnName(1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		String[] colums = {"root.a.b.c1", "root.a.b.c2", "root.a.b.c3"};
		metadata = new TsfileResultMetadata(Arrays.asList(colums), null, null);
		flag = false;
		try {
			metadata.getColumnName(colums.length+1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		flag = false;
		try {
			metadata.getColumnName(0);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		for(int i = 1; i <= colums.length;i++) {
			assertEquals(metadata.getColumnLabel(i), colums[i-1]);
		}
		
	}

	@Test
	public void testGetColumnType() throws SQLException {
		metadata = new TsfileResultMetadata(null, null, null);
		boolean flag = false;
		try {
			metadata.getColumnType(1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		List<String> columnInfoList = new ArrayList<>();
		metadata = new TsfileResultMetadata(columnInfoList, null, null);
		flag = false;
		try {
			metadata.getColumnType(1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		String[] columns = {"timestamp", "root.a.b.boolean", "root.a.b.int32", "root.a.b.int64" , "root.a.b.float", "root.a.b.double", "root.a.b.text"};
		String[] typesString = {"BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"};
		int[] types = {Types.BOOLEAN, Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.DOUBLE, Types.VARCHAR};
		metadata = new TsfileResultMetadata(Arrays.asList(columns), null, Arrays.asList(typesString));
		flag = false;
		try {
			metadata.getColumnType(columns.length+1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		flag = false;
		try {
			metadata.getColumnType(0);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		assertEquals(metadata.getColumnType(1), Types.TIMESTAMP);
		for(int i = 1; i <= types.length; i++) {
			assertEquals(metadata.getColumnType(i+1), types[i-1]);
		}
	}
	
	@Test
	public void testGetColumnTypeName() throws SQLException {
		String operationType = "sum";
		metadata = new TsfileResultMetadata(null, operationType, null);
		assertEquals(metadata.getColumnTypeName(1), operationType);
	}
	
}
