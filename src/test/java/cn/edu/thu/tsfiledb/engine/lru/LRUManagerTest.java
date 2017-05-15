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

import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
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

	private TestLRUManager manager = null;
	private String dirPath = "managerdir";

	@Before
	public void setUp() throws Exception {
		EngineTestHelper.delete(dirPath);
		MetadataManagerHelper.initMetadata();
	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(dirPath);
		MetadataManagerHelper.clearMetadata();
	}

	@Test
	public void test() throws LRUManagerException, InterruptedException {

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
		Thread thread = new Thread(new GetWriterProcessor(deltaObjectId));
		thread.start();
		Thread.sleep(100);
		// the other thread get the write lock for the processor of
		// deltaObjectId1
		processor = manager.getProcessorByLRU(deltaObjectId, true);
		assertEquals(null, processor);
		// the max of the manager is 1, and the processor of deltaObjectId2
		// can't construct
		processor = manager.getProcessorByLRU(deltaObjectId2, true);
		assertEquals(null, processor);
		// the processor of deltaObjectId1 is used, the manager closed completly
		assertEquals(false, manager.close());
		Thread.sleep(1000);

		processor = manager.getProcessorByLRU(deltaObjectId, true);
		assertEquals(false, processor == null);
		processor.writeUnlock();
		assertEquals(true, manager.close());

		// multiple thread read test
		Thread thread2 = new Thread(new GetReaderProcessor(deltaObjectId));
		thread2.start();
		Thread.sleep(100);
		processor = manager.getProcessorByLRU(deltaObjectId, false);
		assertEquals(false, processor == null);
		processor.readUnlock();

	}

	@Test
	public void testCloseMultiProcessor() {

		manager = new TestLRUManager(10, MManager.getInstance(), dirPath);
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

		parameters.put(TEST, action);
		String deltaObjectId = "root.vehicle.d";
		for (int i = 0; i < 3; i++) {
			String tempdeltaObjectId = deltaObjectId + i;
			try {
				TestLRUProcessor processor = manager.getProcessorWithDeltaObjectIdByLRU(tempdeltaObjectId, true,
						parameters);
				processor.writeUnlock();
			} catch (LRUManagerException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}

		try {
			manager.close();
		} catch (LRUManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Get the write
	 */
	class GetWriterProcessor implements Runnable {

		private String deltaObjectId;

		public GetWriterProcessor(String deltaObjectId) {
			this.deltaObjectId = deltaObjectId;
		}

		@Override
		public void run() {
			LRUProcessor lruProcessor = null;
			try {
				lruProcessor = manager.getProcessorByLRU(deltaObjectId, true);
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

		private String deltaObjectId;

		public GetReaderProcessor(String deltaObjectId) {
			this.deltaObjectId = deltaObjectId;
		}

		@Override
		public void run() {
			LRUProcessor lruProcessor = null;

			try {
				lruProcessor = manager.getProcessorByLRU(deltaObjectId, false);
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
