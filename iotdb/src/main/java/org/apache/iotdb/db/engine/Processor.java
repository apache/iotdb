/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.engine.bufferwrite.BufferWriteProcessor;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessor;
import org.apache.iotdb.db.exception.ProcessorException;

/**
 * <p>
 * Processor is used for implementing different processor with different operation.<br>
 *
 * @see BufferWriteProcessor
 * @see FileNodeProcessor
 *
 * @author liukun
 * @author kangrong
 *
 */
// TODO remove this class
public abstract class Processor {

  private final ReadWriteLock lock;
  private String processorName;

  /**
   * Construct processor using name space seriesPath
   *
   * @param processorName
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
   * Get the name space seriesPath
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
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Processor other = (Processor) obj;
    if (processorName == null) {
      if (other.processorName != null) {
        return false;
      }
    } else if (!processorName.equals(other.processorName)) {
      return false;
    }
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
