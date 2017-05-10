package cn.edu.thu.tsfiledb.engine.lru;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.exception.LRUManagerException;
import cn.edu.thu.tsfiledb.engine.lru.LRUManager;
import cn.edu.thu.tsfiledb.engine.lru.LRUProcessor;
import cn.edu.thu.tsfiledb.metadata.MManager;

public class LRUManagerTest {

	private static final String TEST = "TEST";

	class TestLRUProcessor extends LRUProcessor {

		public TestLRUProcessor(String nameSpacePath) {
			super(nameSpacePath);
		}

		@Override
		public boolean canBeClosed() {
			return true;
		}

		@Override
		public void close() throws ProcessorException {

		}

	}

	class TestLRUManager extends LRUManager<TestLRUProcessor> {

		protected TestLRUManager(int maxLRUNumber, MManager mManager, String normalDataDir) {
			super(maxLRUNumber, mManager, normalDataDir);
		}

		@Override
		protected TestLRUProcessor constructNewProcessor(String namespacePath) throws LRUManagerException {
			return new TestLRUProcessor(namespacePath);
		}

		@Override
		protected void initProcessor(TestLRUProcessor processor, String namespacePath, Map<String, Object> parameters)
				throws LRUManagerException {
		}

	}

	TestLRUManager manager = null;

	@Before
	public void setUp() throws Exception {

		MetadataManagerHelper.initMetadata();
	}

	@After
	public void tearDown() throws Exception {

		MetadataManagerHelper.clearMetadata();
	}

	@Test
	public void test() throws LRUManagerException, InterruptedException {
		String dirPath = "managerdir";
		manager = new TestLRUManager(1, MManager.getInstance(), dirPath);
		File dirFile = new File(dirPath);
		assertEquals(true, dirFile.exists());
		assertEquals(true, dirFile.isDirectory());
		assertEquals(dirPath + File.separatorChar, manager.getNormalDataDir());

		Action action = new Action() {

			@Override
			public void act() throws Exception {

			}
		};

		Map<String, Object> parameters = new HashMap<>();
		parameters.put(TEST, action);
		String deltaObjectId = "root.vehicle.d0";
		TestLRUProcessor processor = manager.getProcessorWithDeltaObjectIdByLRU(deltaObjectId, true, parameters);

		// test the lru and getprocessor
		String deltaObjectId2 = "root.vehicle.d1";
		// in the same thread, the thread can get the write lock
		processor = manager.getProcessorByLRU(deltaObjectId2, true);
		assertEquals(false, processor == null);
		processor.writeUnlock();

		// multiple thread write test
		Thread thread = new Thread(new GetWriterProcessor());
		thread.start();
		Thread.sleep(100);
		processor = manager.getProcessorByLRU(deltaObjectId, true);
		assertEquals(null, processor);
		processor = manager.getProcessorByLRU(deltaObjectId2, true);
		assertEquals(null, processor);
		assertEquals(false, manager.close());
		Thread.sleep(1000);
		processor = manager.getProcessorByLRU(deltaObjectId, true);
		assertEquals(false, processor == null);
		processor.writeUnlock();
		assertEquals(true, manager.close());

		// multiple thread read test
		Thread thread2 = new Thread(new GetReaderProcessor());
		thread2.start();
		Thread.sleep(100);
		processor = manager.getProcessorByLRU(deltaObjectId, false);
		assertEquals(false, processor == null);
		processor.readUnlock();

	}

	class GetWriterProcessor implements Runnable {

		@Override
		public void run() {
			LRUProcessor lruProcessor = null;
			try {
				lruProcessor = manager.getProcessorByLRU("root.vehicle.d0", true);
			} catch (LRUManagerException e) {
				e.printStackTrace();
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				lruProcessor.writeUnlock();
			}
		}
	}

	class GetReaderProcessor implements Runnable {

		@Override
		public void run() {
			LRUProcessor lruProcessor = null;

			try {
				lruProcessor = manager.getProcessorByLRU("root.vehicle.d0", false);
			} catch (LRUManagerException e) {
				e.printStackTrace();
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				lruProcessor.readUnlock();
			}
		}
	}

}
