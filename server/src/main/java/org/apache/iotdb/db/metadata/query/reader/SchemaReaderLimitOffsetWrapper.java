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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;

import java.util.NoSuchElementException;

public class SchemaReaderLimitOffsetWrapper<T extends ISchemaInfo> implements ISchemaReader<T> {

  private final ISchemaReader<T> schemaReader;

  private final long limit;
  private final long offset;
  private final boolean hasLimit;

  private int count = 0;
  int curOffset = 0;

  public SchemaReaderLimitOffsetWrapper(ISchemaReader<T> schemaReader, long limit, long offset) {
    this.schemaReader = schemaReader;
    this.limit = limit;
    this.offset = offset;
    this.hasLimit = limit > 0 || offset > 0;

    if (hasLimit) {
      while (curOffset < offset && schemaReader.hasNextFuture()) {
        schemaReader.next();
        curOffset++;
      }
    }
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
  public ListenableFuture<Boolean> hasNextFuture() {
    if (hasLimit) {
      return count < limit && schemaReader.hasNextFuture();
    } else {
      return schemaReader.hasNextFuture();
    }
  }

  @Override
  public T next() {
    if (!hasNextFuture()) {
      throw new NoSuchElementException();
    }
    T result = schemaReader.next();
    if (hasLimit) {
      count++;
    }
    return result;
  }
}
