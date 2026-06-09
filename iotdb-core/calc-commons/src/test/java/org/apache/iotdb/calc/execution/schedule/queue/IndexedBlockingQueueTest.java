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

package org.apache.iotdb.calc.execution.schedule.queue;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class IndexedBlockingQueueTest {

  @Test
  public void testRejectNonPositiveCapacity() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new SimpleQueue(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> new ReserveQueue(-1));
  }

  @Test
  public void testRejectDuplicateIdPush() throws InterruptedException {
    SimpleQueue queue = new SimpleQueue(2);
    Element first = new Element(1);
    Element duplicate = new Element(1);

    queue.push(first);
    Assert.assertThrows(IllegalStateException.class, () -> queue.push(duplicate));
    Assert.assertEquals(1, queue.size());
    Assert.assertSame(first, queue.poll());
    Assert.assertTrue(queue.isEmpty());
  }

  @Test
  public void testRejectRepushWithoutReservedSpace() {
    ReserveQueue queue = new ReserveQueue(1);

    Assert.assertThrows(IllegalStateException.class, () -> queue.repush(new Element(1)));
  }

  @Test
  public void testRejectDecreaseWithoutReservedSpace() {
    ReserveQueue queue = new ReserveQueue(1);

    Assert.assertThrows(IllegalStateException.class, queue::decreaseReservedSize);
  }

  @Test
  public void testReservedSpaceCanOnlyBeReleasedOnce() throws InterruptedException {
    ReserveQueue queue = new ReserveQueue(1);
    Element element = new Element(1);
    queue.push(element);

    Assert.assertSame(element, queue.poll());
    queue.decreaseReservedSize();

    Assert.assertThrows(IllegalStateException.class, queue::decreaseReservedSize);
  }

  private static class SimpleQueue extends IndexedBlockingQueue<Element> {
    private final Queue<Element> elements = new ArrayDeque<>();
    private final Map<ID, Element> keyedElements = new HashMap<>();

    private SimpleQueue(int capacity) {
      super(capacity, new Element(0));
    }

    @Override
    protected Element remove(Element element) {
      Element removed = keyedElements.remove(element.getDriverTaskId());
      if (removed != null) {
        elements.remove(removed);
      }
      return removed;
    }

    @Override
    protected Element get(Element element) {
      return keyedElements.get(element.getDriverTaskId());
    }

    @Override
    public boolean isEmpty() {
      return elements.isEmpty();
    }

    @Override
    protected Element pollFirst() {
      Element first = elements.remove();
      keyedElements.remove(first.getDriverTaskId());
      return first;
    }

    @Override
    protected void pushToQueue(Element element) {
      elements.add(element);
      keyedElements.put(element.getDriverTaskId(), element);
    }

    @Override
    protected boolean contains(Element element) {
      return keyedElements.containsKey(element.getDriverTaskId());
    }

    @Override
    protected void clearAllElements() {
      elements.clear();
      keyedElements.clear();
    }
  }

  private static class ReserveQueue extends IndexedBlockingReserveQueue<Element> {
    private final Queue<Element> elements = new ArrayDeque<>();
    private final Map<ID, Element> keyedElements = new HashMap<>();

    private ReserveQueue(int capacity) {
      super(capacity, new Element(0));
    }

    @Override
    protected Element remove(Element element) {
      Element removed = keyedElements.remove(element.getDriverTaskId());
      if (removed != null) {
        elements.remove(removed);
      }
      return removed;
    }

    @Override
    protected Element get(Element element) {
      return keyedElements.get(element.getDriverTaskId());
    }

    @Override
    public boolean isEmpty() {
      return elements.isEmpty();
    }

    @Override
    protected Element pollFirst() {
      Element first = elements.remove();
      keyedElements.remove(first.getDriverTaskId());
      return first;
    }

    @Override
    protected void pushToQueue(Element element) {
      elements.add(element);
      keyedElements.put(element.getDriverTaskId(), element);
    }

    @Override
    protected boolean contains(Element element) {
      return keyedElements.containsKey(element.getDriverTaskId());
    }

    @Override
    protected void clearAllElements() {
      elements.clear();
      keyedElements.clear();
    }
  }

  private static class Element implements IDIndexedAccessible {
    private ElementId id;

    private Element(int id) {
      this.id = new ElementId(id);
    }

    @Override
    public ID getDriverTaskId() {
      return id;
    }

    @Override
    public void setId(ID id) {
      this.id = (ElementId) id;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof Element && ((Element) obj).id.equals(id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
  }

  private static class ElementId implements ID {
    private final int id;

    private ElementId(int id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof ElementId && ((ElementId) obj).id == id;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(id);
    }
  }
}
