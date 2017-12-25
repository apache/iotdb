package cn.edu.tsinghua.iotdb.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTThreadFactory implements ThreadFactory {

	private static final AtomicInteger poolNumber = new AtomicInteger(1);
	private final ThreadGroup group;
	private final AtomicInteger threadNumber = new AtomicInteger(1);
	private final String namePrefix;

	public IoTThreadFactory(String poolName) {
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
		// thread pool name format : pool-number-IoTDB-poolName-thread-
		this.namePrefix = "pool-" + poolNumber.getAndIncrement() + "-IoTDB" + "-" + poolName + "-thread-";
	}

	@Override
	public Thread newThread(Runnable r) {
		// thread name format : pool-number-IoTDB-poolName-thread-threadnum
		Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
		if (t.isDaemon())
			t.setDaemon(false);
		if (t.getPriority() != Thread.NORM_PRIORITY)
			t.setPriority(Thread.NORM_PRIORITY);
		return t;
	}
}
