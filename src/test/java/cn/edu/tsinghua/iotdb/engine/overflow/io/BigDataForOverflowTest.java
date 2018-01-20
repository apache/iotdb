package cn.edu.tsinghua.iotdb.engine.overflow.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.filenode.FilterUtilsForOverflow;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.series.ISeriesWriter;

/**
 * @author liukun
 *
 */
public class BigDataForOverflowTest {

	private String nameSpacePath = "root.vehicle.d0";
	private String overflowfilePath = null;
	private String overflowrestorefilePath = null;
	private String overflowmergefilePath = null;
	private Map<String, Object> parameters = null;
	private OverflowProcessor ofprocessor = null;
	//private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();
	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
	private TsfileDBConfig tsfileDBConfig = TsfileDBDescriptor.getInstance().getConfig();
	private String deltaObjectId = "root.vehicle.d0";
	private String measurementId = "s0";

	private Action overflowflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("overflow flush action");
		}
	};

	private Action filenodeflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("filenode flush action");
		}
	};

	private Action filenodemanagerbackupaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("filenode manager backup action");
		}
	};

	private Action filenodemanagerflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("filenode manager flush action");
		}
	};

	//private String overflowDataDir = null;
	private int rowGroupSize;
	private long overflowFlushThreshold;
	
	@Before
	public void setUp() throws Exception {

		parameters = new HashMap<String, Object>();
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowflushaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, filenodeflushaction);
		
		//origin value
		rowGroupSize = tsconfig.groupSizeInByte;
		overflowFlushThreshold = tsfileDBConfig.overflowFileSizeThreshold;
		//new value
		tsconfig.groupSizeInByte = 1024 * 1024 * 10;
		tsfileDBConfig.overflowMetaSizeThreshold = 1024*1024;
		
		
		overflowfilePath = new File(PathUtils.getOverflowWriteDir(nameSpacePath),nameSpacePath+".overflow").getPath();
		overflowrestorefilePath = overflowfilePath + ".restore";
		overflowmergefilePath = overflowfilePath + ".merge";
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		//recovery value
		tsconfig.groupSizeInByte = rowGroupSize;
		tsfileDBConfig.overflowFileSizeThreshold = overflowFlushThreshold;
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testBigData() {

		long step = 10000;
		long pass = step * 10;
		long length = step * 1000;
		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		for (long i = 1; i <= length; i++) {
			if (i > 0 && i % pass == 0) {
				System.out.println(i / pass + " pass");
			}
			try {
				TSRecord record = new TSRecord(i, deltaObjectId);
				record.addTuple(DataPoint.getDataPoint(TSDataType.INT64, measurementId, String.valueOf(i)));
				ofprocessor.insert(record);
			} catch (OverflowProcessorException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		// construct the time filter for the query
		SingleSeriesFilterExpression timeFilter = FilterUtilsForOverflow.construct(null, null, "0",
				"(>=" + 0 + ")&" + "(<=" + length + ")");
		List<Object> queryResult = ofprocessor.query(deltaObjectId, measurementId, timeFilter, null, null,
				TSDataType.INT64);
		DynamicOneColumnData insertData = (DynamicOneColumnData) queryResult.get(0);
		assertEquals(length, insertData.valueLength);
		try {
			ofprocessor.close();
		} catch (OverflowProcessorException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}

		try {
			assertEquals(true, new File(overflowrestorefilePath).exists());
			ofprocessor.switchWorkingToMerge();
			assertEquals(true, new File(overflowmergefilePath).exists());
			assertEquals(true, new File(overflowfilePath).exists());
		} catch (OverflowProcessorException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			ofprocessor.insert(deltaObjectId, measurementId, length + 1, TSDataType.INT64, String.valueOf(length) + 1);
		} catch (OverflowProcessorException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}

		queryResult = ofprocessor.query(deltaObjectId, measurementId, timeFilter, null, null, TSDataType.INT64);
		insertData = (DynamicOneColumnData) queryResult.get(0);
		assertEquals(length, insertData.valueLength);
		try {
			ofprocessor.switchMergeToWorking();
			ofprocessor.close();
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
