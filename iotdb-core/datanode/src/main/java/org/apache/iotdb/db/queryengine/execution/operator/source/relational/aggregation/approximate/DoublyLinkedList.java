/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

public class DoublyLinkedList<T> implements Iterable<T> {

  protected int size;
  protected ListNode2<T> tail;
  protected ListNode2<T> head;

  /** Append to head of list */
  public ListNode2<T> add(T value) {
    ListNode2<T> node = new ListNode2<T>(value);
    if (size++ == 0) {
      tail = node;
    } else {
      node.prev = head;
      head.next = node;
    }

    head = node;

    return node;
  }

  /** Prepend to tail of list */
  public ListNode2<T> enqueue(T value) {
    ListNode2<T> node = new ListNode2<T>(value);
    if (size++ == 0) {
      head = node;
    } else {
      node.next = tail;
      tail.prev = node;
    }

    tail = node;

    return node;
  }

  public void add(ListNode2<T> node) {
    node.prev = head;
    node.next = null;

    if (size++ == 0) {
      tail = node;
    } else {
      head.next = node;
    }

    head = node;
  }

  public ListNode2<T> addAfter(ListNode2<T> node, T value) {
    ListNode2<T> newNode = new ListNode2<T>(value);
    addAfter(node, newNode);
    return newNode;
  }

  public void addAfter(ListNode2<T> node, ListNode2<T> newNode) {
    newNode.next = node.next;
    newNode.prev = node;
    node.next = newNode;
    if (newNode.next == null) {
      head = newNode;
    } else {
      newNode.next.prev = newNode;
    }
    size++;
  }

  public void remove(ListNode2<T> node) {
    if (node == tail) {
      tail = node.next;
    } else {
      node.prev.next = node.next;
    }

    if (node == head) {
      head = node.prev;
    } else {
      node.next.prev = node.prev;
    }
    size--;
  }

  public int size() {
    return size;
  }

  @Override
  public Iterator<T> iterator() {
    return new DoublyLinkedListIterator(this);
  }

  protected class DoublyLinkedListIterator implements Iterator<T> {

    protected DoublyLinkedList<T> list;
    protected ListNode2<T> itr;
    protected int length;

    public DoublyLinkedListIterator(DoublyLinkedList<T> list) {
      this.length = list.size;
      this.list = list;
      this.itr = list.tail;
    }

    @Override
    public boolean hasNext() {
      return itr != null;
    }

    @Override
    public T next() {
      if (length != list.size) {
        throw new ConcurrentModificationException();
      }
      T next = itr.value;
      itr = itr.next;
      return next;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public T first() {
    return tail == null ? null : tail.getValue();
  }

  public T last() {
    return head == null ? null : head.getValue();
  }

  public ListNode2<T> head() {
    return head;
  }

  public ListNode2<T> tail() {
    return tail;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  @SuppressWarnings("unchecked")
  public T[] toArray() {
    T[] a = (T[]) new Object[size];
    int i = 0;
    for (T v : this) {
      a[i++] = v;
    }
    return a;
  }
}
