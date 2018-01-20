package cn.edu.tsinghua.iotdb.engine;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowProcessor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

/**
 * <p>
 * Processor is used for implementing different processor with different
 * operation.<br>
 * 
 * @see BufferWriteProcessor
 * @see OverflowProcessor
 * @see FileNodeProcessor
 * 
 * @author liukun
 * @author kangrong
 *
 */
public abstract class Processor {
	private String processorName;
	private final ReadWriteLock lock;

	/**
	 * Construct processor using name space path
	 * 
	 * @param nameSpacePath
	 */
	public Processor(String processorName) {
		this.processorName = processorName;
		this.lock = new ReentrantReadWriteLock();
	}

	/**
	 * Release the read lock
	 */
	public void readUnlock() {
		lock.readLock().unlock();
	}

	/**
	 * Acquire the read lock
	 */
	public void readLock() {
		lock.readLock().lock();
	}

	/**
	 * Acquire the write lock
	 */
	public void writeLock() {
		lock.writeLock().lock();
	}

	/**
	 * Release the write lock
	 */
	public void writeUnlock() {
		lock.writeLock().unlock();
	}

	/**
	 * @param isWriteLock
	 *            true acquire write lock, false acquire read lock
	 */
	public void lock(boolean isWriteLock) {
		if (isWriteLock) {
			lock.writeLock().lock();
		} else {
			lock.readLock().lock();
		}
	}

	public boolean tryLock(boolean isWriteLock) {
		if (isWriteLock) {
			return tryWriteLock();
		} else {
			return tryReadLock();
		}
	}

	/**
	 * @param isWriteUnlock
	 *            true release write lock, false release read unlock
	 */
	public void unlock(boolean isWriteUnlock) {
		if (isWriteUnlock) {
			writeUnlock();
		} else {
			readUnlock();
		}
	}

	/**
	 * Get the name space path
	 * 
	 * @return
	 */
	public String getProcessorName() {
		return processorName;
	}

	/**
	 * Try to get the write lock
	 * 
	 * @return
	 */
	public boolean tryWriteLock() {
		return lock.writeLock().tryLock();
	}

	/**
	 * Try to get the read lock
	 * 
	 * @return
	 */
	public boolean tryReadLock() {
		return lock.readLock().tryLock();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((processorName == null) ? 0 : processorName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Processor other = (Processor) obj;
		if (processorName == null) {
			if (other.processorName != null)
				return false;
		} else if (!processorName.equals(other.processorName))
			return false;
		return true;
	}

	/**
	 * Judge whether this processor can be closed.
	 *
	 * @return true if subclass doesn't have other implementation.
	 */
	public abstract boolean canBeClosed();
	
	public abstract boolean flush() throws IOException;

	/**
	 * Close the processor.<br>
	 * Notice: Thread is not safe
	 * 
	 * @throws IOException
	 * @throws ProcessorException
	 */
	public abstract void close() throws ProcessorException;
	
	public abstract long memoryUsage();
}
