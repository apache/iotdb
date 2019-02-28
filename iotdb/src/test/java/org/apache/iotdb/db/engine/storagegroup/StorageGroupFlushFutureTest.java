/**
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

package org.apache.iotdb.db.engine.storagegroup;

import static org.mockito.Matchers.any;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageGroupFlushFutureTest {

  StorageGroupFlushFuture future1;
  StorageGroupFlushFuture future2;
  StorageGroupFlushFuture future3;
  StorageGroupFlushFuture future4;

  @Before
  public void setUp() throws Exception {
    future1 = new StorageGroupFlushFuture(new CanCanceledFuture(), new CanCanceledFuture());
    future2 = new StorageGroupFlushFuture(new CanNotCanceledFuture(), new CanNotCanceledFuture());
    future3 = new StorageGroupFlushFuture(new CanCanceledFuture(), new CanNotCanceledFuture());
    future4 = new StorageGroupFlushFuture(new CanNotCanceledFuture(), new CanCanceledFuture());
  }

  @After
  public void tearDown() throws Exception {
    //nothing
  }

  @Test
  public void cancel() {
    Assert.assertTrue(future1.cancel(true));
    Assert.assertFalse(future2.cancel(true));
    Assert.assertFalse(future3.cancel(true));
    Assert.assertFalse(future4.cancel(true));
  }

  @Test
  public void isCancelled() {
    Assert.assertFalse(future1.isCancelled());
    Assert.assertFalse(future2.isCancelled());
    Assert.assertFalse(future3.isCancelled());
    Assert.assertFalse(future4.isCancelled());

    future1.cancel(true);
    future2.cancel(true);
    future3.cancel(true);
    future4.cancel(true);

    Assert.assertTrue(future1.isCancelled());
    Assert.assertFalse(future2.isCancelled());
    Assert.assertFalse(future3.isCancelled());
    Assert.assertFalse(future4.isCancelled());
  }

  @Test
  public void isDone() throws ExecutionException, InterruptedException, TimeoutException {
    Assert.assertFalse(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertFalse(future3.isDone());
    Assert.assertFalse(future4.isDone());

    future1.get();
    future2.get();
    future3.get(2, TimeUnit.MILLISECONDS);
    future4.get(2, TimeUnit.MILLISECONDS);

    Assert.assertTrue(future1.isDone());
    Assert.assertTrue(future2.isDone());
    Assert.assertTrue(future3.isDone());
    Assert.assertTrue(future4.isDone());
  }

  @Test
  public void isHasOverflowFlushTask() {
    Assert.assertTrue(future1.isHasOverflowFlushTask());
  }


  private class CanCanceledFuture implements Future<Boolean> {
    boolean cancel = false;
    boolean done = false;
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancel = true;
      done = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return cancel;
    }

    @Override
    public boolean isDone() {
      if (cancel || done) {
        return true;
      }
      return false;
    }

    @Override
    public Boolean get() {
      done = true;
      return true;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) {
      done =true;
      return true;
    }
  }

  private class CanNotCanceledFuture implements Future<Boolean> {
    boolean done = false;
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public Boolean get() {
      done = true;
      return true;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) {
      done =true;
      return true;
    }
  }
}