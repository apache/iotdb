package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.engine.lru.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class BufferWriteProcessorTest {

	Action bfflushaction = new Action() {

		@Override
		public void act() throws Exception {

		}
	};

	Action bfcloseaction = new Action() {

		@Override
		public void act() throws Exception {
		}
	};

	Action fnflushaction = new Action() {

		@Override
		public void act() throws Exception {

		}
	};

	BufferWriteProcessor processor = null;
	String nsp = "root.vehicle.d0";
	String nps2 = "root.vehicle.d1";

	private boolean cachePageData = false;
	private int groupSizeInByte;
	private int pageCheckSizeThreshold;
	private int	pageSizeInByte;
	private int	maxStringLength;
	private TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();

	@Before
	public void setUp() throws Exception {
		//origin value
		cachePageData = TsFileConf.duplicateIncompletedPage;
		groupSizeInByte = TsFileConf.groupSizeInByte;
		pageCheckSizeThreshold = TsFileConf.pageCheckSizeThreshold;
		pageSizeInByte = TsFileConf.pageSizeInByte;
		maxStringLength = TsFileConf.maxStringLength;
		//new value
		TsFileConf.duplicateIncompletedPage = true;
		TsFileConf.groupSizeInByte = 2000;
		TsFileConf.pageCheckSizeThreshold = 3;
		TsFileConf.pageSizeInByte = 100;
		TsFileConf.maxStringLength = 2;
		// init metadata
		MetadataManagerHelper.initMetadata();
	}

	@After
	public void tearDown() throws Exception {
		//recovery value
		TsFileConf.duplicateIncompletedPage = cachePageData;
		TsFileConf.groupSizeInByte = groupSizeInByte;
		TsFileConf.pageCheckSizeThreshold = pageCheckSizeThreshold;
		TsFileConf.pageSizeInByte = pageSizeInByte;
		TsFileConf.maxStringLength = maxStringLength;
		//clean environment
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testMultipleRowgroup() throws BufferWriteProcessorException, IOException {
		String filename = "bufferwritetest";
		Map<String, Object> parameters = new HashMap<>();
		parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
		parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);

		try {
			processor = new BufferWriteProcessor(nsp, filename, parameters);
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		File nspdir = PathUtils.getBufferWriteDir(nsp);
		assertEquals(true, nspdir.isDirectory());
		for (int i = 0; i < 1000; i++) {
			processor.write(nsp, "s0", 100, TSDataType.INT32, i + "");
			processor.write(nps2, "s0", 100, TSDataType.INT32, i + "");
			if (i == 200) {
				break;
			}
		}
		// wait to flush end
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// query
		Pair<List<Object>, List<RowGroupMetaData>> pair = processor.getIndexAndRowGroupList(nsp, "s0");
		int size = pair.right.size();
		pair = processor.getIndexAndRowGroupList(nps2, "s0");
		assertEquals(size, pair.right.size());
		processor.close();
	}

	@Test
	public void testBufferwrite() throws IOException, BufferWriteProcessorException {
		String filename = "bufferwritetest";
		BufferWriteProcessor bufferWriteProcessor1 = null;
		BufferWriteProcessor bufferWriteProcessor2 = null;
		Map<String, Object> parameters = new HashMap<>();
		parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
		parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);

		File restorefile = new File(PathUtils.getBufferWriteDir(nsp), filename + ".restore");
		try {
			bufferWriteProcessor1 = new BufferWriteProcessor(nsp, filename, parameters);
			processor = bufferWriteProcessor1;
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// check dir
		File nspdir = PathUtils.getBufferWriteDir(nsp);
		assertEquals(true, nspdir.isDirectory());
		// check outfile
		// write record and test multiple thread flush rowgroup
		for (int i = 0; i < 1000; i++) {
			processor.write(nsp, "s0", 100, TSDataType.INT32, i + "");
			if (i == 400) {
				break;
			}
		}
		// wait to flush end
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// write some bytes in the outputfile and test cuf off function
		File dir = PathUtils.getBufferWriteDir(nsp);
		File outFile = new File(dir, filename);
		ITsRandomAccessFileWriter raf = new TsRandomAccessFileWriter(outFile);
		raf.seek(outFile.length());
		byte[] buff = new byte[100];
		Arrays.fill(buff, (byte) 10);
		raf.write(buff);
		raf.close();
		// read the buffer write file from middle of the file and test the cut
		// off function
		assertEquals(true, restorefile.exists());
		processor = new BufferWriteProcessor(nsp, filename, parameters);
		bufferWriteProcessor2 = processor;
		Pair<List<Object>, List<RowGroupMetaData>> pair = processor.getIndexAndRowGroupList(nsp, "s0");
		DynamicOneColumnData columnData = (DynamicOneColumnData) pair.left.get(0);
		Pair<List<ByteArrayInputStream>, CompressionTypeName> right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) pair.left
				.get(1);
		assertEquals(null, columnData);
		assertEquals(null, right);
		int lastRowGroupNum = pair.right.size();
		for (int i = 0; i < 1000; i++) {
			processor.write(nsp, "s0", 100, TSDataType.INT32, i + "");
			if (i == 400) {
				break;
			}
		}
		// wait to flush end
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		pair = processor.getIndexAndRowGroupList(nsp, "s0");
		columnData = (DynamicOneColumnData) pair.left.get(0);
		right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) pair.left.get(1);
		assertEquals(false, columnData == null);
		assertEquals(false, right == null);
		System.out.println(columnData.valueLength);
		System.out.println(right.left.size() + " " + right.right);
		processor = new BufferWriteProcessor(nsp, filename, parameters);
		pair = processor.getIndexAndRowGroupList(nsp, "s0");
		// assert the number of rowgroup
		assertEquals(lastRowGroupNum * 2, pair.right.size());
		processor.write(nsp, "s0", 100, TSDataType.INT32, 100 + "");
		bufferWriteProcessor1.close();
		bufferWriteProcessor2.close();
		processor.close();
		assertEquals(false, restorefile.exists());
	}

	@Test
	public void testNoDataBufferwriteRecovery() throws BufferWriteProcessorException {
		String filename = "bufferwritetest";
		BufferWriteProcessor bufferWriteProcessor1 = null;
		Map<String, Object> parameters = new HashMap<>();
		parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
		parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);
	
		try {
			bufferWriteProcessor1 = new BufferWriteProcessor(nsp, filename, parameters);
			processor = bufferWriteProcessor1;
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		File nspdir = PathUtils.getBufferWriteDir(nsp);
		assertEquals(true, nspdir.isDirectory());
		for (int i = 0; i < 1000; i++) {
			processor.write(nsp, "s0", 100, TSDataType.INT32, i + "");
			if (i == 1) {
				break;
			}
		}
		processor = new BufferWriteProcessor(nsp, filename, parameters);
		processor.close();
		bufferWriteProcessor1.close();
	}
}
