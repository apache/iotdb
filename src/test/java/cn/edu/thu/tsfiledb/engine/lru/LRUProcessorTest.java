package cn.edu.thu.tsfiledb.engine.lru;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

/**
 * @author liukun
 *
 */
public class LRUProcessorTest {
	
	
	class TestLRUProcessor extends LRUProcessor{

		public TestLRUProcessor(String nameSpacePath) {
			super(nameSpacePath);
		}

		@Override
		public boolean canBeClosed() {
			return false;
		}

		@Override
		public void close() throws ProcessorException {
			
		}
		
	}

	TestLRUProcessor processor1;
	TestLRUProcessor processor2;
	TestLRUProcessor processor3;
	
	private TsfileDBConfig dbconfig = TsfileDBDescriptor.getInstance().getConfig();
	
	@Before
	public void setUp() throws Exception {
		dbconfig.metadataDir = "metadata";
		EngineTestHelper.delete(dbconfig.metadataDir);
		processor1 = new TestLRUProcessor("ns1");
		processor2 = new TestLRUProcessor("ns2");
		processor3 = new TestLRUProcessor("ns1");
	}

	@After
	public void tearDown() throws Exception {
		MManager.getInstance().flushObjectToFile();
		EngineTestHelper.delete(dbconfig.metadataDir);
	}

	@Test
	public void testEquals() {
		assertEquals(processor1, processor3);
		assertFalse(processor1.equals(processor2));
	}
	
	@Test
	public void testLockAndUnlock() throws InterruptedException{
		Thread thread = new Thread(new lockRunnable());

		thread.start();
		
		Thread.sleep(100);
		
		assertEquals(false, processor1.tryReadLock());
		assertEquals(false, processor1.tryLock(true));
		
		Thread.sleep(2000);
		
		assertEquals(true, processor1.tryLock(true));
		assertEquals(true, processor1.tryLock(false));
		
		processor1.readUnlock();
		processor1.writeUnlock();
		
		Thread thread2 = new Thread(new readLockRunable());
		thread2.start();
		Thread.sleep(100);
		
		assertEquals(false, processor1.tryWriteLock());
		assertEquals(true, processor1.tryReadLock());
		
		Thread.sleep(1500);
		assertEquals(false, processor1.tryWriteLock());
		processor1.readUnlock();
		assertEquals(true, processor1.tryWriteLock());
		processor1.writeUnlock();
	}
	
	class lockRunnable implements Runnable{

		@Override
		public void run() {
			processor1.lock(true);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			processor1.unlock(true);
		}
	}
	
	class readLockRunable implements Runnable{

		@Override
		public void run() {
			processor1.readLock();
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			processor1.readUnlock();
		}
		
	}
}
