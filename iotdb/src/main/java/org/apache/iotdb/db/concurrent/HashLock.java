package org.apache.iotdb.db.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class manages an array of locks and use the hash code of an object as an index of the array to find the corresponding lock,
 * so that the operations on the same key (string) can be prevented while the number of locks remain controlled.
 */
public class HashLock {
    private static final int DEFAULT_LOCK_NUM = 100;

    private ReentrantReadWriteLock[] locks;
    private int lockSize;

    public HashLock() {
        this.lockSize = DEFAULT_LOCK_NUM;
        init();
    }

    public HashLock(int lockSize) {
        if(lockSize <= 0)
            lockSize = DEFAULT_LOCK_NUM;
        this.lockSize = lockSize;
        init();
    }

    private void init() {
        locks = new ReentrantReadWriteLock[lockSize];
        for(int i = 0; i < lockSize; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    public void readLock(Object obj) {
        this.locks[Math.abs(obj.hashCode()) % lockSize].readLock().lock();
    }

    public void readUnlock(Object obj) {
        this.locks[Math.abs(obj.hashCode()) % lockSize].readLock().unlock();
    }

    public void writeLock(Object obj) {
        this.locks[Math.abs(obj.hashCode()) % lockSize].writeLock().lock();
    }

    public void writeUnlock(Object obj) {
        this.locks[Math.abs(obj.hashCode()) % lockSize].writeLock().unlock();
    }

    /**
     * This method will unlock all locks. Only for test convenience.
     */
    public void reset() {
        for(int i = 0; i < lockSize; i++) {
            try {
                locks[i].readLock().unlock();
            } catch (Exception ignored) {
            }
            try {
                locks[i].writeLock().unlock();
            } catch (Exception ignored) {
            }
        }
    }
}
