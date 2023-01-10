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
package org.apache.iotdb.db.mpp.common.schematree.visitor;

import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class SchemaTreeVisitorWithLimitOffsetWrapper<R> extends SchemaTreeVisitor<R> {
  private final SchemaTreeVisitor<R> visitor;
  private final int limit;
  private final int offset;
  private final boolean hasLimit;

  private int count = 0;
  int curOffset = 0;

  public SchemaTreeVisitorWithLimitOffsetWrapper(
      SchemaTreeVisitor<R> visitor, int limit, int offset) {
    this.visitor = visitor;
    this.limit = limit;
    this.offset = offset;
    hasLimit = limit > 0 || offset > 0;

    if (hasLimit) {
      while (curOffset < offset && visitor.hasNext()) {
        visitor.next();
        curOffset++;
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (hasLimit) {
      return count < limit && visitor.hasNext();
    } else {
      return visitor.hasNext();
    }
  }

  @Override
  public R next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    R result = visitor.next();
    if (hasLimit) {
      count++;
    }
    return result;
  }

  @Override
  public void close() {
    visitor.close();
  }

  @Override
  public List<R> getAllResult() {
    List<R> result = new ArrayList<>();
    while (hasNext()) {
      result.add(next());
    }
    return result;
  }

  @Override
  protected boolean acceptInternalMatchedNode(SchemaNode node) {
    // do nothing
    return false;
  }

  @Override
  protected boolean acceptFullMatchedNode(SchemaNode node) {
    // do nothing
    return false;
  }

  @Override
  protected R generateResult(SchemaNode nextMatchedNode) {
    // do nothing
    return null;
  }

  @Override
  public void reset() {
    visitor.reset();
    count = 0;
    curOffset = 0;
    if (hasLimit) {
      while (curOffset < offset && visitor.hasNext()) {
        visitor.next();
        curOffset++;
      }
    }
  }

  public int getNextOffset() {
    return curOffset + count;
  }
}
