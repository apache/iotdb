/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.timecho.iotdb.commons.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class SecureAsynchronousFileChannel extends AsynchronousFileChannel {

  private final SecureFileSystemProvider provider;
  private final AsynchronousFileChannel delegate;
  private final boolean secureEraseEligible;

  SecureAsynchronousFileChannel(
      SecureFileSystemProvider provider,
      AsynchronousFileChannel delegate,
      boolean secureEraseEligible) {
    this.provider = provider;
    this.delegate = delegate;
    this.secureEraseEligible = secureEraseEligible;
  }

  @Override
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public AsynchronousFileChannel truncate(long size) throws IOException {
    provider.truncate(delegate, secureEraseEligible, size);
    return this;
  }

  @Override
  public void force(boolean metaData) throws IOException {
    delegate.force(metaData);
  }

  @Override
  public <A> void lock(
      long position,
      long size,
      boolean shared,
      A attachment,
      CompletionHandler<FileLock, ? super A> handler) {
    Objects.requireNonNull(handler);
    delegate.lock(
        position,
        size,
        shared,
        attachment,
        new CompletionHandler<FileLock, A>() {
          @Override
          public void completed(FileLock result, A completedAttachment) {
            handler.completed(wrap(result), completedAttachment);
          }

          @Override
          public void failed(Throwable exc, A completedAttachment) {
            handler.failed(exc, completedAttachment);
          }
        });
  }

  @Override
  public Future<FileLock> lock(long position, long size, boolean shared) {
    return new SecureFileLockFuture(delegate.lock(position, size, shared));
  }

  @Override
  public FileLock tryLock(long position, long size, boolean shared) throws IOException {
    return wrap(delegate.tryLock(position, size, shared));
  }

  @Override
  public <A> void read(
      ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
    delegate.read(dst, position, attachment, handler);
  }

  @Override
  public Future<Integer> read(ByteBuffer dst, long position) {
    return delegate.read(dst, position);
  }

  @Override
  public <A> void write(
      ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
    delegate.write(src, position, attachment, handler);
  }

  @Override
  public Future<Integer> write(ByteBuffer src, long position) {
    return delegate.write(src, position);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  private FileLock wrap(FileLock lock) {
    return lock == null ? null : new SecureFileLock(this, lock);
  }

  private final class SecureFileLockFuture implements Future<FileLock> {

    private final Future<FileLock> delegateFuture;
    private FileLock wrappedLock;

    private SecureFileLockFuture(Future<FileLock> delegateFuture) {
      this.delegateFuture = delegateFuture;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return delegateFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return delegateFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegateFuture.isDone();
    }

    @Override
    public FileLock get() throws InterruptedException, ExecutionException {
      return wrapOnce(delegateFuture.get());
    }

    @Override
    public FileLock get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return wrapOnce(delegateFuture.get(timeout, unit));
    }

    private synchronized FileLock wrapOnce(FileLock lock) {
      if (lock == null) {
        return null;
      }
      if (wrappedLock == null) {
        wrappedLock = wrap(lock);
      }
      return wrappedLock;
    }
  }
}
