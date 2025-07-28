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

package org.apache.iotdb.session.subscription.payload;

import org.apache.tsfile.write.record.Tablet;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class SubscriptionSessionDataSetsHandler
    implements Iterable<SubscriptionSessionDataSet>, SubscriptionMessageHandler {

  private final Map<String, List<Tablet>> tablets;

  public SubscriptionSessionDataSetsHandler(final Map<String, List<Tablet>> tablets) {
    this.tablets = tablets;
  }

  @Override
  public Iterator<SubscriptionSessionDataSet> iterator() {
    return new Iterator<SubscriptionSessionDataSet>() {
      // Iterator over map entries: databaseName -> list of tablets
      private final Iterator<Map.Entry<String, List<Tablet>>> entryIterator =
          tablets.entrySet().iterator();
      // Current databaseName
      private String currentDatabase;
      // Iterator over the current list of tablets
      private Iterator<Tablet> tabletIterator = Collections.emptyIterator();

      @Override
      public boolean hasNext() {
        // Advance to next non-empty tablet list if needed
        while (!tabletIterator.hasNext() && entryIterator.hasNext()) {
          Map.Entry<String, List<Tablet>> entry = entryIterator.next();
          currentDatabase = entry.getKey();
          List<Tablet> list = entry.getValue();
          tabletIterator = (list != null ? list.iterator() : Collections.emptyIterator());
        }
        return tabletIterator.hasNext();
      }

      @Override
      public SubscriptionSessionDataSet next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Tablet tablet = tabletIterator.next();
        return new SubscriptionSessionDataSet(tablet, currentDatabase);
      }
    };
  }

  public Iterator<Tablet> tabletIterator() {
    return new Iterator<Tablet>() {
      final Iterator<SubscriptionSessionDataSet> iterator = iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Tablet next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return iterator.next().getTablet();
      }
    };
  }
}
