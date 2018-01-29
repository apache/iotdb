package cn.edu.tsinghua.iotdb.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class IoTDBThreadPoolFactoryTest {
	private final String POOL_NAME = "test";
	private AtomicInteger count;
	
	@Before
	public void setUp() throws Exception {
		count = new AtomicInteger(0);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNewFixedThreadPool() throws InterruptedException, ExecutionException {
		String reason = "NewFixedThreadPool";
		Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
		int threadCount = 5;
		ExecutorService exec = IoTDBThreadPoolFactory.newFixedThreadPool(threadCount, POOL_NAME, handler);
		for(int i = 0; i < threadCount;i++){
			Runnable task = new TestThread(reason);
			exec.execute(task);
		}
		Thread.sleep(100);
		assertEquals(count.get(), threadCount);
	}

	@Test
	public void testNewSingleThreadExecutor() throws InterruptedException {
		String reason = "NewSingleThreadExecutor";
		Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
		int threadCount = 1;
		ExecutorService exec = IoTDBThreadPoolFactory.newSingleThreadExecutor(POOL_NAME, handler);
		for(int i = 0; i < threadCount;i++){
			Runnable task = new TestThread(reason);
			exec.execute(task);
		}
		Thread.sleep(100);
		assertEquals(count.get(), threadCount);
	}

	@Test
	public void testNewCachedThreadPool() throws InterruptedException {
		String reason = "NewCachedThreadPool";
		Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
		int threadCount = 10;
		ExecutorService exec = IoTDBThreadPoolFactory.newCachedThreadPool(POOL_NAME, handler);
		for(int i = 0; i < threadCount;i++){
			Runnable task = new TestThread(reason);
			exec.execute(task);
		}
		Thread.sleep(100);
		assertEquals(count.get(), threadCount);
	}

	@Test
	public void testNewSingleThreadScheduledExecutor() throws InterruptedException {
		String reason = "NewSingleThreadScheduledExecutor";
		Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
		int threadCount = 1;
		ScheduledExecutorService exec = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(POOL_NAME, handler);
		for(int i = 0; i < threadCount;i++){
			Runnable task = new TestThread(reason);
			ScheduledFuture<?> future = exec.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
			try {
				future.get();
			} catch (ExecutionException e) {
				assertEquals(reason, e.getCause().getMessage());
				count.addAndGet(1);
			}
		}
		Thread.sleep(100);
		assertEquals(count.get(), threadCount);
	}

	@Test
	public void testNewScheduledThreadPool() throws InterruptedException {
		String reason = "NewScheduledThreadPool";
		Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
		int threadCount = 10;
		ScheduledExecutorService exec = IoTDBThreadPoolFactory.newScheduledThreadPool(threadCount, POOL_NAME, handler);
		for(int i = 0; i < threadCount; i++){
			Runnable task = new TestThread(reason);
			ScheduledFuture<?> future = exec.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);
			try {
				future.get();
			} catch (ExecutionException e) {
				assertEquals(reason, e.getCause().getMessage());
				count.addAndGet(1);
			}
		}
		Thread.sleep(1000);
		exec.shutdown();
		assertEquals(count.get(), threadCount);
	}

	@Test
	public void testCreateJDBCClientThreadPool() throws InterruptedException {
		String reason = "CreateJDBCClientThreadPool";
		TThreadPoolServer.Args args = new Args(null);
		args.maxWorkerThreads = 100;
		args.minWorkerThreads = 10;
		args.stopTimeoutVal = 10;
		args.stopTimeoutUnit = TimeUnit.SECONDS;
		Thread.UncaughtExceptionHandler handler = new TestExceptionHandler(reason);
		int threadCount = 500;
		ExecutorService exec = IoTDBThreadPoolFactory.createJDBCClientThreadPool(args, POOL_NAME, handler);
		for(int i = 0; i < threadCount; i++){
			Runnable task = new TestThread(reason);
			exec.execute(task);
		}
		Thread.sleep(1000);
		exec.shutdown();
		assertEquals(count.get(), threadCount);
	}
	
	class TestExceptionHandler implements Thread.UncaughtExceptionHandler {
		
		private String name;
		
		public TestExceptionHandler(String name) {
			this.name = name;
		}
		
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			assertEquals(name, e.getMessage());
			count.addAndGet(1);
		}
	}
	
	class TestThread implements Runnable{
		private String name;
		
		public TestThread(String name) {
			this.name = name;
		}
		
		@Override
		public void run() {
			throw new RuntimeException(name);
		}
		
	}
}
