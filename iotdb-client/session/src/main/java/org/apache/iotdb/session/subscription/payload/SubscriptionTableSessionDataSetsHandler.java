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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class SubscriptionTableSessionDataSetsHandler
    implements Iterable<SubscriptionTableSessionDataSet>, SubscriptionMessageHandler {

  private final Map<String, List<Tablet>> tablets;

  public SubscriptionTableSessionDataSetsHandler(final Map<String, List<Tablet>> tablets) {
    this.tablets = tablets;
  }

  @Override
  public Iterator<SubscriptionTableSessionDataSet> iterator() {
    return new Iterator<SubscriptionTableSessionDataSet>() {
      private final Iterator<Map.Entry<String, List<Tablet>>> entryIterator =
          tablets.entrySet().iterator();
      private String currentKey;
      private Iterator<Tablet> tabletIterator;

      @Override
      public boolean hasNext() {
        // If there is no current tablet iterator, or it is exhausted, try to fetch the next.
        while ((tabletIterator == null || !tabletIterator.hasNext()) && entryIterator.hasNext()) {
          Map.Entry<String, List<Tablet>> entry = entryIterator.next();
          currentKey = entry.getKey();
          tabletIterator = entry.getValue().iterator();
        }
        return tabletIterator != null && tabletIterator.hasNext();
      }

      @Override
      public SubscriptionTableSessionDataSet next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Tablet tablet = tabletIterator.next();
        return new SubscriptionTableSessionDataSet(tablet, currentKey);
      }
    };
  }

  public Iterator<Map.Entry<String, List<Tablet>>> tabletIterator() {
    return tablets.entrySet().iterator();
  }
}
