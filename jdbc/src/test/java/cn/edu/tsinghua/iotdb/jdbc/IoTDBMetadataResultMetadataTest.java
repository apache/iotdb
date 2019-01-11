package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBMetadataResultMetadataTest {

	private IoTDBMetadataResultMetadata metadata;
	private String[] cols = {"a1", "a2", "a3", "a4"};
	
	@Before
	public void setUp() throws Exception {
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetColumnCount() throws SQLException {
		boolean flag = false;
		try {
			metadata = new IoTDBMetadataResultMetadata(null);
			assertEquals((long)metadata.getColumnCount(), 0);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		flag = false;
		try {
			String[] nullArray = {};
			metadata = new IoTDBMetadataResultMetadata(nullArray);
			assertEquals((long)metadata.getColumnCount(), 0);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		metadata = new IoTDBMetadataResultMetadata(cols);
		assertEquals((long)metadata.getColumnCount(), cols.length);
	}


	@Test
	public void testGetColumnName() throws SQLException {
		boolean flag = false;
		metadata = new IoTDBMetadataResultMetadata(null);
		try {
			metadata.getColumnName(1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		try {
			String[] nullArray = {};
			metadata = new IoTDBMetadataResultMetadata(nullArray);
			metadata.getColumnName(1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		metadata = new IoTDBMetadataResultMetadata(cols);
		try {
			metadata.getColumnName(0);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);
		
		flag = false;
		try {
			metadata.getColumnName(cols.length+1);
		} catch (Exception e) {
			flag = true;
		}
		assertEquals(flag, true);

		for(int i = 1; i <= cols.length; i++){
			assertEquals(metadata.getColumnName(i), cols[i-1]);
		}
		
	}

}
