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

package org.apache.iotdb.db.metadata.query.reader;

import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.NoSuchElementException;

public class SchemaReaderLimitOffsetWrapper<T extends ISchemaInfo> implements ISchemaReader<T> {

  private final ISchemaReader<T> schemaReader;

  private final long limit;
  private final long offset;
  private final boolean hasLimit;

  private int count = 0;
  private int curOffset = 0;
  private ListenableFuture<?> isBlocked = null;

  public SchemaReaderLimitOffsetWrapper(ISchemaReader<T> schemaReader, long limit, long offset) {
    this.schemaReader = schemaReader;
    this.limit = limit;
    this.offset = offset;
    this.hasLimit = limit > 0 || offset > 0;
  }

  @Override
  public boolean isSuccess() {
    return schemaReader.isSuccess();
  }

  @Override
  public Throwable getFailure() {
    return schemaReader.getFailure();
  }

  @Override
  public void close() throws Exception {
    schemaReader.close();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (isBlocked != null) {
      return isBlocked;
    }
    isBlocked = tryGetNext();
    return isBlocked;
  }

  private ListenableFuture<?> tryGetNext() {
    if (hasLimit) {
      if (curOffset < offset) {
        // first time
        return Futures.submit(
            () -> {
              while (curOffset < offset && schemaReader.hasNext()) {
                schemaReader.next();
                curOffset++;
              }
              return schemaReader.hasNext();
            },
            FragmentInstanceManager.getInstance().getIntoOperationExecutor());
      }
      if (count >= limit) {
        return NOT_BLOCKED_FALSE;
      } else {
        return schemaReader.isBlocked();
      }
    } else {
      return schemaReader.isBlocked();
    }
  }

  @Override
  public boolean hasNext() {
    try {
      isBlocked().get();
      return schemaReader.hasNext();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    T result = schemaReader.next();
    if (hasLimit) {
      count++;
    }
    return result;
  }
}
