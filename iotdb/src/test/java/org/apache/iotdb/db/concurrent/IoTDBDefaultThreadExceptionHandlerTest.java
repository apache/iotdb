package org.apache.iotdb.db.concurrent;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class IoTDBDefaultThreadExceptionHandlerTest {
	private Thread.UncaughtExceptionHandler handler;
	private AtomicInteger count;
	private final String message = "Expected!";

	@Before
	public void setUp() throws Exception {
		handler = Thread.getDefaultUncaughtExceptionHandler();
		Thread.setDefaultUncaughtExceptionHandler(new TestExceptionHandler(message));
		count = new AtomicInteger(0);
	}

	@After
	public void tearDown() throws Exception {
		Thread.setDefaultUncaughtExceptionHandler(handler);
	}

	@Test
	public void test() throws InterruptedException {
		int num = 10;
		for(int i = 0; i < num; i++){
			TestThread thread = new TestThread();
			thread.start();
		}
		Thread.sleep(500);
		assertEquals(num, count.get());
	}

	class TestThread extends Thread{
		public void run() {
	      throw new RuntimeException(message);
	    }
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
}
