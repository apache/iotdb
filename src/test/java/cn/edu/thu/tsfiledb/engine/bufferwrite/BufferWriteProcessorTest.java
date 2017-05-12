package cn.edu.thu.tsfiledb.engine.bufferwrite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.conf.TSFileDBConfig;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.MetadataManagerHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;

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
	@Before
	public void setUp() throws Exception {
		EngineTestHelper.delete(nsp);
		MetadataManagerHelper.initMetadata();
	}

	@After
	public void tearDown() throws Exception {
		MetadataManagerHelper.clearMetadata();
		EngineTestHelper.delete(nsp);
	}

	@Test
	public void test() throws IOException, BufferWriteProcessorException {
		String filename = "bufferwritetest";
		Map<String, Object> parameters = new HashMap<>();
		parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
		parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);
		TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
		TSFileDBConfig tsdbconfig = TSFileDBDescriptor.getInstance().getConfig();
		tsdbconfig.BufferWriteDir = "";
		tsconfig.rowGroupSize = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSize = 100;
		tsconfig.defaultMaxStringLength = 2;
		File outputfile = new File(nsp + File.separatorChar + filename);
		File restorefile = new File(nsp + File.separatorChar + filename + ".restore");
		if (outputfile.exists()) {
			outputfile.delete();
		}
		try {
			processor = new BufferWriteProcessor(nsp, filename, parameters);
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail("Not yet implemented");
		}

		// check dir
		File nspdir = new File(nsp);
		assertEquals(true, nspdir.isDirectory());
		// check outfile
		// write record and test multiple thread flush rowgroup
		for (int i = 0; i < 1000; i++) {
			processor.write(nsp, "s0", 100, TSDataType.INT32, i + "");
			if(i==400){
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
		File dir = new File(nsp);
		File outFile = new File(dir, filename);
		TSRandomAccessFileWriter raf = new RandomAccessOutputStream(outFile);
		raf.seek(outFile.length());
		byte[] buff = new byte[100];
		Arrays.fill(buff, (byte)10);
		raf.write(buff);
		raf.close();
		// read the buffer write file from middle of the file and test the cut off function
		assertEquals(true, restorefile.exists());
		processor = new BufferWriteProcessor(nsp, filename, parameters);
		Pair<DynamicOneColumnData, List<RowGroupMetaData>> pair = processor.getIndexAndRowGroupList(nsp, "s0");
		assertEquals(0, pair.left.length);
		int lastRowGroupNum = pair.right.size();
		for (int i = 0; i < 1000; i++) {
			processor.write(nsp, "s0", 100, TSDataType.INT32, i + "");
			if(i==400){
				break;
			}
		}
		// wait to flush end
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		processor = new BufferWriteProcessor(nsp, filename, parameters);
		pair = processor.getIndexAndRowGroupList(nsp, "s0");
		// assert the number of rowgroup 
		assertEquals(lastRowGroupNum*2, pair.right.size());
		processor.write(nsp, "s0", 100, TSDataType.INT32, 100 + "");
		processor.close();
		assertEquals(false, restorefile.exists());
	}
}
