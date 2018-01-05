package cn.edu.tsinghua.iotdb.engine.overflow.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * This is the test class for {@link OverflowSupport}
 * 
 * @author liukun
 *
 */
/**
 * @author liukun
 *
 */
public class OverflowSupportTest {

	private String filePath = "overflowsupportTest";
	private String fileBackupPath = filePath + ".backup";
	private String deltaObjectId = "root.vehicle.d0";

	// should add boolean and enum
	private String[] measurementIds = { "s0", "s1", "s2", "s3", "s4", "s5", "s6" };
	private TSDataType[] dataTypes = { TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
			TSDataType.BOOLEAN, TSDataType.TEXT, TSDataType.ENUMS };
	private OverflowFileIO ofio = null;
	private OverflowReadWriter ofrw = null;
	private OverflowSupport ofsupport = null;
	//private TsfileDBConfig tsFileDBConfig = TsfileDBDescriptor.getInstance().getConfig();

	@Before
	public void setUp() throws Exception {
		ofsupport = null;
		ofrw = null;
		ofio = null;
		MetadataManagerHelper.initMetadata();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		ofsupport = null;
		ofrw = null;
		ofio = null;
		EnvironmentUtils.cleanDir(fileBackupPath);
		EnvironmentUtils.cleanDir(filePath);
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testFlushCloseAndMergeQuery() {

		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowreadwriter failed, the reason is " + e.getMessage());
		}

		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfileio failed, the reason is " + e.getMessage());
		}

		try {
			ofsupport = new OverflowSupport(ofio, null);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowsupport failed, the reason is " + e.getMessage());
		}

		// insert data 1,2,3
		insertData(ofsupport, deltaObjectId, measurementIds[0], 1, 1);
		insertData(ofsupport, deltaObjectId, measurementIds[0], 2, 2);
		insertData(ofsupport, deltaObjectId, measurementIds[0], 5, 5);
		// update data
		updateData(ofsupport, deltaObjectId, measurementIds[0], 100, 4, 6);
		// query
		List<Object> result = ofsupport.query(deltaObjectId, measurementIds[0], null, null, null, dataTypes[0]);
		DynamicOneColumnData insertData = (DynamicOneColumnData) result.get(0);
		assertEquals(3, insertData.valueLength);
		// check time check value
		assertEquals(1, insertData.getTime(0));
		assertEquals(1, insertData.getInt(0));

		assertEquals(2, insertData.getTime(1));
		assertEquals(2, insertData.getInt(1));

		assertEquals(5, insertData.getTime(2));
		assertEquals(100, insertData.getInt(2));

		try {
			ofio.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void insertData(OverflowSupport ofSupport, String deltaObjectId, String measurementId, int value,
			long timestamp) {
		ofSupport.insert(deltaObjectId, measurementId, timestamp, TSDataType.INT32, BytesUtils.intToBytes(value));
		ofSupport.switchWorkToFlush();
		try {
			ofSupport.flushRowGroupToStore();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	public void updateData(OverflowSupport ofSupport, String deltaObjectId, String measurementId, int value,
			long startTime, long endTime) {
		ofSupport.update(deltaObjectId, measurementId, startTime, endTime, TSDataType.INT32,
				BytesUtils.intToBytes(value));
		ofSupport.switchWorkToFlush();
		try {
			ofSupport.flushRowGroupToStore();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testNoDataClose() {
		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowreadwriter failed, the reason is " + e.getMessage());
		}

		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfileio failed, the reason is " + e.getMessage());
		}

		try {
			ofsupport = new OverflowSupport(ofio, null);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowsupport failed, the reason is " + e.getMessage());
		}

		try {
			assertEquals(-1, ofsupport.endFile());
		} catch (IOException e1) {
			e1.printStackTrace();
			fail("end file failed, the reason is " + e1.getMessage());
		}
	}

	@Test
	public void testRestoreFrommetdata() {

		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowreadwriter failed, the reason is " + e.getMessage());
		}

		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfileio failed, the reason is " + e.getMessage());
		}

		try {
			ofsupport = new OverflowSupport(ofio, null);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowsupport failed, the reason is " + e.getMessage());
		}

		// add data
		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[0], i, dataTypes[0], BytesUtils.intToBytes(i));
		}

		for (int i = 1; i < 20; i = i + 2) {
			ofsupport.update(deltaObjectId, measurementIds[1], i, i + 1, dataTypes[1], BytesUtils.longToBytes(i));
		}

		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[2], i, dataTypes[2],
					BytesUtils.floatToBytes((float) (i * 1.5)));
		}

		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[3], i, dataTypes[3],
					BytesUtils.doubleToBytes((double) (i * 1.5)));
		}

		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[5], i, dataTypes[5], BytesUtils.StringToBytes("tsfile" + i));
		}

		ofsupport.switchWorkToFlush();
		try {
			ofsupport.flushRowGroupToStore();
		} catch (IOException e) {
			e.printStackTrace();
			fail("flush rowgroup to store failed");
		}
		OFFileMetadata ofFileMetadata = null;
		try {
			ofFileMetadata = ofsupport.getOFFileMetadata();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Get the OFFileMetadata failed, the reason is " + e.getMessage());
		}
		long lastRowgroupPosition = 0;
		try {
			lastRowgroupPosition = ofsupport.getPos();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Get the overflow support postion failed, the reason is " + e.getMessage());
		}
		try {
			ofio.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Close the overflowfileio failed, the reason is " + e.getMessage());
		}
		// test the file length
		assertEquals(lastRowgroupPosition, new File(filePath).length());
		// add data from the lastRowgroupPosition
		try {
			ofrw = new OverflowReadWriter(filePath);
			ofrw.seek(lastRowgroupPosition);
			ofrw.write(new byte[10]);
			ofrw.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Add data from the lastRowgroupPosition failed, the reason is " + e.getMessage());
		}
		assertEquals(lastRowgroupPosition + 10, new File(filePath).length());

		// test to cuf off file
		try {
			ofrw = new OverflowReadWriter(filePath);
			ofio = new OverflowFileIO(ofrw, filePath, lastRowgroupPosition);
			ofsupport = new OverflowSupport(ofio, ofFileMetadata);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Restore overflow support from offilemedata failed, the reason is " + e.getMessage());
		}
		// int
		List<Object> result = ofsupport.query(deltaObjectId, measurementIds[0], null, null, null, dataTypes[0]);
		DynamicOneColumnData insertData = (DynamicOneColumnData) result.get(0);
		assertEquals(10, insertData.valueLength);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals(i, insertData.getInt(i - 1));
		}
		// float
		result = ofsupport.query(deltaObjectId, measurementIds[2], null, null, null, dataTypes[2]);
		insertData = (DynamicOneColumnData) result.get(0);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals(String.valueOf(i * 1.5), String.valueOf(insertData.getFloat(i - 1)));
		}
		// double
		result = ofsupport.query(deltaObjectId, measurementIds[3], null, null, null, dataTypes[3]);
		insertData = (DynamicOneColumnData) result.get(0);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals(String.valueOf(i * 1.5), String.valueOf(insertData.getDouble(i - 1)));
		}
		// string
		result = ofsupport.query(deltaObjectId, measurementIds[5], null, null, null, dataTypes[5]);
		insertData = (DynamicOneColumnData) result.get(0);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals("tsfile" + i, insertData.getStringValue(i - 1));
		}
		// long
		result = ofsupport.query(deltaObjectId, measurementIds[1], null, null, null, dataTypes[1]);
		DynamicOneColumnData updateData = (DynamicOneColumnData) result.get(1);
		assertEquals(10, updateData.valueLength);
		assertEquals(20, updateData.timeLength);
		for (int i = 1; i < 20; i = i + 2) {
			assertEquals(i, updateData.getTime(i - 1));
			assertEquals(i + 1, updateData.getTime(i));
			assertEquals(i, updateData.getLong(i / 2));
		}

		ofsupport.switchWorkToFlush();
		try {
			ofsupport.flushRowGroupToStore();
			ofsupport.endFile();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush the rowgroup failed or close the file failed, the reason is " + e.getMessage());
		}
	}

	@Test
	public void testAllDataType() {
		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowreadwriter failed, the reason is " + e.getMessage());
		}

		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfileio failed, the reason is " + e.getMessage());
		}

		try {
			ofsupport = new OverflowSupport(ofio, null);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowsupport failed, the reason is " + e.getMessage());
		}

		assertEquals(0, ofsupport.calculateMemSize());
		try {
			assertEquals(0, ofsupport.getPos());
		} catch (IOException e) {
			e.printStackTrace();
			fail("Get the position of the overflow support failed, the reason is " + e.getMessage());
		}
		// test query: the deltaObjectId or the measurementId is not exist.
		Object timeFilter = new SingleSeriesFilterExpression() {

			@Override
			public FilterSeries<?> getFilterSeries() {
				return null;
			}

			@Override
			public <T> T accept(FilterVisitor<T> vistor) {
				return null;
			}
		};

		List<Object> result = ofsupport.query(deltaObjectId, measurementIds[0],
				(SingleSeriesFilterExpression) timeFilter, null, null, dataTypes[0]);
		assertEquals(timeFilter, result.get(3));
		// test insert: int
		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[0], i, dataTypes[0], BytesUtils.intToBytes(i));
		}
		// query data
		result = ofsupport.query(deltaObjectId, measurementIds[0], null, null, null, dataTypes[0]);
		DynamicOneColumnData insertData = (DynamicOneColumnData) result.get(0);
		assertEquals(10, insertData.valueLength);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals(i, insertData.getInt(i - 1));
		}
		// test insert: float
		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[2], i, dataTypes[2],
					BytesUtils.floatToBytes((float) (i * 1.5)));
		}
		result = ofsupport.query(deltaObjectId, measurementIds[2], null, null, null, dataTypes[2]);
		insertData = (DynamicOneColumnData) result.get(0);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals(String.valueOf(i * 1.5), String.valueOf(insertData.getFloat(i - 1)));
		}
		// test insert: double
		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[3], i, dataTypes[3],
					BytesUtils.doubleToBytes((double) (i * 1.5)));
		}
		result = ofsupport.query(deltaObjectId, measurementIds[3], null, null, null, dataTypes[3]);
		insertData = (DynamicOneColumnData) result.get(0);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals(String.valueOf(i * 1.5), String.valueOf(insertData.getDouble(i - 1)));
		}

		// test insert: byte_array
		for (int i = 1; i < 11; i++) {
			ofsupport.insert(deltaObjectId, measurementIds[5], i, dataTypes[5], BytesUtils.StringToBytes("tsfile" + i));
		}
		result = ofsupport.query(deltaObjectId, measurementIds[5], null, null, null, dataTypes[5]);
		insertData = (DynamicOneColumnData) result.get(0);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertData.getTime(i - 1));
			assertEquals("tsfile" + i, insertData.getStringValue(i - 1));
		}

		// test update: long
		for (int i = 1; i < 20; i = i + 2) {
			ofsupport.update(deltaObjectId, measurementIds[1], i, i + 1, dataTypes[1], BytesUtils.longToBytes(i));
		}
		// query data
		result = ofsupport.query(deltaObjectId, measurementIds[1], null, null, null, dataTypes[1]);
		DynamicOneColumnData updateData = (DynamicOneColumnData) result.get(1);
		assertEquals(10, updateData.valueLength);
		assertEquals(20, updateData.timeLength);
		for (int i = 1; i < 20; i = i + 2) {
			assertEquals(i, updateData.getTime(i - 1));
			assertEquals(i + 1, updateData.getTime(i));
			assertEquals(i, updateData.getLong(i / 2));
		}
		// test delete
		ofsupport.delete(deltaObjectId, measurementIds[0], 20, dataTypes[0]);
		result = ofsupport.query(deltaObjectId, measurementIds[0], null, null, null, dataTypes[0]);
		insertData = (DynamicOneColumnData) result.get(0);
		assertEquals(0, insertData.valueLength);
		assertEquals(0, insertData.timeLength);
		// test flush
		ofsupport.switchWorkToFlush();
		try {
			ofsupport.flushRowGroupToStore();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush rowgroup to store failed, the reason is " + e.getMessage());
		}
		OFFileMetadata ofFileMetadata = null;
		try {
			ofFileMetadata = ofsupport.getOFFileMetadata();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Get the of filemetadata failed, the reason is " + e.getMessage());
		}
		assertEquals(0, ofFileMetadata.getLastFooterOffset());
		assertEquals(1, ofFileMetadata.getMetaDatas().size());
		assertEquals(deltaObjectId, ofFileMetadata.getMetaDatas().get(0).getDeltaObjectId());
		assertEquals(5, ofFileMetadata.getMetaDatas().get(0).getMetaDatas().size());

		// test update: boolean
		// bug: and not support
		for (int i = 1; i < 20; i = i + 2) {
			ofsupport.update(deltaObjectId, measurementIds[4], i, i + 1, dataTypes[4],
					BytesUtils.boolToBytes(i / 2 == 0 ? true : false));
		}
		// query data
		result = ofsupport.query(deltaObjectId, measurementIds[4], null, null, null, dataTypes[4]);
		updateData = (DynamicOneColumnData) result.get(1);
		for (int i = 1; i < 20; i = i + 2) {
			assertEquals(i, updateData.getTime(i - 1));
			assertEquals(i + 1, updateData.getTime(i));
			assertEquals(i / 2 == 0 ? true : false, updateData.getBoolean(i / 2));
		}
		// test enum type
		// bug: and not support
		// test end file
		try {
			ofsupport.endFile();
		} catch (IOException e) {
			e.printStackTrace();
			fail("End file failed, the reason is " + e.getMessage());
		}
	}

}
