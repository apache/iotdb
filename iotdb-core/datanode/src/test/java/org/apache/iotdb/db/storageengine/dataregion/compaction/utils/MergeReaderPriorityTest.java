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

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.MergeReaderPriority;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class MergeReaderPriorityTest {
  @Test
  public void test() {
    List<MergeReaderPriority> priorities = new ArrayList<>();
    // Create MergeReaderPriority objects with different attributes
    priorities.add(new MergeReaderPriority(1000L, 2L, 10L, true));
    priorities.add(new MergeReaderPriority(1000L, 2L, 5L, true));
    priorities.add(new MergeReaderPriority(1000L, 1L, 10L, true));
    priorities.add(new MergeReaderPriority(1000L, 3L, 15L, false));
    priorities.add(new MergeReaderPriority(1000L, 2L, 15L, false));

    // Add these objects to a PriorityQueue with the comparison logic
    PriorityQueue<MergeReaderPriority> queue = new PriorityQueue<>(priorities);

    // Test the order of elements in the queue
    Assert.assertEquals(queue.poll(), priorities.get(2)); // seq, lowest version
    Assert.assertEquals(queue.poll(), priorities.get(1)); // seq, same version, lower offset
    Assert.assertEquals(queue.poll(), priorities.get(0)); // seq, higher version, higher offset
    Assert.assertEquals(queue.poll(), priorities.get(4)); // unseq, lower version, higher offset
    Assert.assertEquals(queue.poll(), priorities.get(3)); // unseq, highest version, highest offset
  }

  @Test
  public void testCompactionQueue() {
    List<PriorityPoint> priorities = new ArrayList<>();
    // Create MergeReaderPriority objects with different attributes

    // Add these objects to a PriorityQueue with the comparison logic
    PriorityQueue<PriorityPoint> queue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.time, o2.time);
              return timeCompare != 0 ? timeCompare : o2.priority.compareTo(o1.priority);
            });
    priorities.add(new PriorityPoint(1, new MergeReaderPriority(1000L, 2L, 10L, true)));
    priorities.add(new PriorityPoint(1, new MergeReaderPriority(1000L, 2L, 5L, true)));
    priorities.add(new PriorityPoint(1, new MergeReaderPriority(1000L, 1L, 10L, true)));
    priorities.add(new PriorityPoint(1, new MergeReaderPriority(1000L, 3L, 15L, false)));
    priorities.add(new PriorityPoint(1, new MergeReaderPriority(1000L, 2L, 15L, false)));
    priorities.add(new PriorityPoint(11, new MergeReaderPriority(1000L, 2L, 10L, true)));
    queue.addAll(priorities);

    // Test the order of elements in the queue
    Assert.assertEquals(queue.poll(), priorities.get(3)); // unseq, highest version, highest offset
    Assert.assertEquals(queue.poll(), priorities.get(4)); // unseq, lower version, higher offset
    Assert.assertEquals(queue.poll(), priorities.get(0)); // seq, higher version, higher offset
    Assert.assertEquals(queue.poll(), priorities.get(1)); // seq, same version, lower offset
    Assert.assertEquals(queue.poll(), priorities.get(2)); // seq, lowest version
    Assert.assertEquals(queue.poll(), priorities.get(5)); // largest timestamp
  }

  private static class PriorityPoint {
    long time;
    MergeReaderPriority priority;

    public PriorityPoint(long time, MergeReaderPriority priority) {
      this.time = time;
      this.priority = priority;
    }

    @Override
    public String toString() {
      return "PriorityPoint{" + "time=" + time + ", priority=" + priority + '}';
    }
  }
}
