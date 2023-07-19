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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response;

import org.apache.iotdb.lsm.response.BaseResponse;
import org.apache.iotdb.lsm.response.IQueryResponse;

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class QueryResponse extends BaseResponse<RoaringBitmap>
    implements IQueryResponse<RoaringBitmap, Integer> {

  private final DeviceIDIterator deviceIDIterator;

  public QueryResponse() {
    super(new RoaringBitmap());
    deviceIDIterator = new DeviceIDIterator();
  }

  @Override
  public void or(IQueryResponse<RoaringBitmap, Integer> queryResponse) {
    getValue().or(queryResponse.getValue());
  }

  @Override
  public void and(IQueryResponse<RoaringBitmap, Integer> queryResponse) {
    getValue().and(queryResponse.getValue());
  }

  @Override
  public Iterator<Integer> getIterator() {
    if (getValue() != null && !getValue().isEmpty()) {
      deviceIDIterator.addIterator(getValue().iterator());
    }
    return deviceIDIterator;
  }

  @Override
  public void addIterator(Iterator<Integer> iterator) {
    deviceIDIterator.addIterator(iterator);
  }

  /** Support for iterative access to results */
  private class DeviceIDIterator implements Iterator<Integer> {

    private List<Iterator<Integer>> iterators;

    private Integer next;

    private int index;

    public DeviceIDIterator() {
      iterators = new ArrayList<>();
      next = null;
      index = 0;
    }

    /**
     * Returns {@code true} if the iteration has more elements. (In other words, returns {@code
     * true} if {@link #next} would return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      }
      while (index < iterators.size()) {
        Iterator<Integer> iterator = iterators.get(index);
        if (iterator.hasNext()) {
          next = iterator.next();
          return true;
        }
        index++;
      }
      return false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Integer next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      int now = next;
      next = null;
      return now;
    }

    public void addIterator(Iterator<Integer> iterator) {
      iterators.add(iterator);
    }
  }
}
