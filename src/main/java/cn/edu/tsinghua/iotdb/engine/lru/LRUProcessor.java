package cn.edu.tsinghua.iotdb.engine.lru;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

/**
 * <p>
 * LRUProcessor is used for implementing different processor with different
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
public abstract class LRUProcessor {
	private final static Logger LOGGER = LoggerFactory.getLogger(LRUProcessor.class);
	protected String nameSpacePath;
	private final ReadWriteLock lock;

	/**
	 * Construct processor using name space path
	 * 
	 * @param nameSpacePath
	 */
	public LRUProcessor(String nameSpacePath) {
		this.nameSpacePath = nameSpacePath;
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
	public String getNameSpacePath() {
		return nameSpacePath;
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
		result = prime * result + ((nameSpacePath == null) ? 0 : nameSpacePath.hashCode());
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
		LRUProcessor other = (LRUProcessor) obj;
		if (nameSpacePath == null) {
			if (other.nameSpacePath != null)
				return false;
		} else if (!nameSpacePath.equals(other.nameSpacePath))
			return false;
		return true;
	}

	/**
	 * Judge whether this processor can be closed.
	 *
	 * @return true if subclass doesn't have other implementation.
	 */
	public abstract boolean canBeClosed();

	/**
	 * Close the processor.<br>
	 * Notice: Thread is not safe
	 * 
	 * @throws IOException
	 * @throws ProcessorException
	 */
	public abstract void close() throws ProcessorException;
}
