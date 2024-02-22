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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.page;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class FIFOPageContainer implements IPageContainer {

  private final ConcurrentLinkedDeque<Pair<Integer, ISchemaPage>> pageQueue =
      new ConcurrentLinkedDeque<>();
  private final AtomicInteger totalSize =
      new AtomicInteger(0); // total size of pages, because calculate size is too expensive

  @Override
  public void put(int regionId, ISchemaPage page) {
    pageQueue.add(new Pair<>(regionId, page));
    totalSize.incrementAndGet();
  }

  @Override
  public void iterateToRemove(Predicate<Pair<Integer, ISchemaPage>> predicate, int targetSize) {
    Iterator<Pair<Integer, ISchemaPage>> iterator = pageQueue.iterator();
    while (iterator.hasNext()) {
      if (totalSize.get() <= targetSize) {
        break; // consider concurrent situation
      }
      Pair<Integer, ISchemaPage> pair = iterator.next();
      if (pair.right.getRefCnt().get() == 0 && predicate.test(pair)) {
        iterator.remove();
        totalSize.decrementAndGet();
        if (totalSize.get() <= targetSize) {
          break;
        }
      }
    }
  }

  @Override
  public int size() {
    return totalSize.get();
  }

  @TestOnly
  @Override
  public Iterator<Pair<Integer, ISchemaPage>> iterator() {
    return pageQueue.iterator();
  }

  @TestOnly
  @Override
  public void clear() {
    pageQueue.clear();
    totalSize.set(0);
  }
}
