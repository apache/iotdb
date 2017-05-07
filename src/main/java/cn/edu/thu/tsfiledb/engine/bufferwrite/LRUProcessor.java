package cn.edu.thu.tsfiledb.engine.bufferwrite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 * LRUProcessor is used for implementing different processor with different
 * operation.<br>
 * 
 * @see BufferWriteProcessor
 * @see OverflowProcessor
 * @see FileNodeProcessor
 * 
 * @author kangrong
 *
 */
public abstract class LRUProcessor {
	private final static Logger LOG = LoggerFactory.getLogger(LRUProcessor.class);
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
		LOG.debug("{}: lru read unlock-Thread id {}", this.getClass().getSimpleName(), Thread.currentThread().getId());
		lock.readLock().unlock();
	}

	/**
	 * Acquire the read lock
	 */
	public void readLock() {
		LOG.debug("{}: lru read lock-Thread id {}", this.getClass().getSimpleName(), Thread.currentThread().getId());
		lock.readLock().lock();
	}

	/**
	 * Acquire the write lock
	 */
	public void writeLock() {
		LOG.debug("{}: lru write lock-Thread id {}", this.getClass().getSimpleName(), Thread.currentThread().getId());
		lock.writeLock().lock();
	}

	/**
	 * Release the write lock
	 */
	public void writeUnlock() {
		LOG.debug("{}: lru write lock-Thread id {}", this.getClass().getSimpleName(), Thread.currentThread().getId());
		lock.writeLock().unlock();
	}

	/**
	 * @param isWriteLock
	 *            true acquire write lock, false acquire read lock
	 */
	public void lock(boolean isWriteLock) {
		if (isWriteLock) {
			LOG.debug("{}: lru write lock-Thread id {}", this.getClass().getSimpleName(),
					Thread.currentThread().getId());
			lock.writeLock().lock();
		} else {
			LOG.debug("{}: lru read lock-Thread id {}", this.getClass().getSimpleName(),
					Thread.currentThread().getId());
			lock.readLock().lock();
		}

	}

	/**
	 * @param isWriteUnlock
	 *            true release write lock, false release read unlock
	 */
	public void unlock(boolean isWriteUnlock) {
		if (isWriteUnlock) {
			LOG.debug("{}: lru write unlock-Thread id {}", this.getClass().getSimpleName(),
					Thread.currentThread().getId());
			lock.writeLock().unlock();
		} else {
			LOG.debug("{}: lru read unlock-Thread id {}", this.getClass().getSimpleName(),
					Thread.currentThread().getId());
			lock.readLock().unlock();
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

	/**
	 * Judge whether this processor can be closed.
	 *
	 * @return true if subclass doesn't have other implementation.
	 */
	public abstract boolean canBeClosed();

	@Override
	public int hashCode() {
		return nameSpacePath.hashCode();
	}

	@Override
	public boolean equals(Object pro) {
		if (pro == null)
			return false;
		if (!pro.getClass().equals(this.getClass()))
			return false;
		LRUProcessor lruPro = (LRUProcessor) pro;
		return nameSpacePath.equals(lruPro.getNameSpacePath());
	}

	/**
	 * Close the processor.<br>
	 * Notice: Thread is not safe
	 */
	public abstract void close();
}
