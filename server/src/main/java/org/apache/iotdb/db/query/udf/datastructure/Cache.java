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

package org.apache.iotdb.db.query.udf.datastructure;

/** <b>Note: It's not thread safe.</b> */
public abstract class Cache {

  private static class Node {

    private Node previous;
    private int value;
    private Node succeeding;

    private Node() {}

    private Node remove() {
      previous.succeeding = succeeding;
      succeeding.previous = previous;
      return this;
    }

    private void set(Node previous, int value, Node succeeding) {
      this.previous = previous;
      previous.succeeding = this;
      this.value = value;
      this.succeeding = succeeding;
      succeeding.previous = this;
    }
  }

  protected final Node head;
  protected final Node tail;

  protected final Node[] cachedNodes;

  protected final int cacheCapacity;
  protected int cacheSize;

  protected Cache(int capacity) {
    head = new Node();
    tail = new Node();
    head.succeeding = tail;
    tail.previous = head;

    cachedNodes = new Node[capacity];
    for (int i = 0; i < capacity; ++i) {
      cachedNodes[i] = new Node();
    }

    cacheCapacity = capacity;
    cacheSize = 0;
  }

  protected boolean removeFirstOccurrence(int value) {
    for (Node node = head.succeeding; node != tail; node = node.succeeding) {
      if (node.value == value) {
        cachedNodes[--cacheSize] = node.remove();
        return true;
      }
    }
    return false;
  }

  protected int removeLast() {
    Node last = tail.previous.remove();
    cachedNodes[--cacheSize] = last;
    return last.value;
  }

  protected void addFirst(int value) {
    cachedNodes[cacheSize++].set(head, value, head.succeeding);
  }

  public void clear() {
    while (cacheSize != 0) {
      removeLast();
    }
  }
}
