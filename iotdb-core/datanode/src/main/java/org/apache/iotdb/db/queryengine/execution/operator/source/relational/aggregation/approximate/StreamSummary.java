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

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Based on the <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i> data structure as
 * described in: <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i> by
 * Metwally, Agrawal, and Abbadi
 *
 * @param <T> type of data in the stream to be summarized
 */
public class StreamSummary<T> implements ITopK<T>, Externalizable {

  protected class Bucket {

    protected DoublyLinkedList<Counter<T>> counterList;

    private long count;

    public Bucket(long count) {
      this.count = count;
      this.counterList = new DoublyLinkedList<Counter<T>>();
    }
  }

  protected int capacity;
  private HashMap<T, ListNode2<Counter<T>>> counterMap;
  protected DoublyLinkedList<Bucket> bucketList;

  /**
   * @param capacity maximum size (larger capacities improve accuracy)
   */
  public StreamSummary(int capacity) {
    this.capacity = capacity;
    counterMap = new HashMap<T, ListNode2<Counter<T>>>();
    bucketList = new DoublyLinkedList<Bucket>();
  }

  public int getCapacity() {
    return capacity;
  }

  /**
   * Algorithm: <i>Space-Saving</i>
   *
   * @param item stream element (<i>e</i>)
   * @return false if item was already in the stream summary, true otherwise
   */
  @Override
  public boolean offer(T item) {
    return offer(item, 1);
  }

  /**
   * Algorithm: <i>Space-Saving</i>
   *
   * @param item stream element (<i>e</i>)
   * @return false if item was already in the stream summary, true otherwise
   */
  @Override
  public boolean offer(T item, int incrementCount) {
    return offerReturnAll(item, incrementCount).left;
  }

  /**
   * @param item stream element (<i>e</i>)
   * @return item dropped from summary if an item was dropped, null otherwise
   */
  public T offerReturnDropped(T item, int incrementCount) {
    return offerReturnAll(item, incrementCount).right;
  }

  /**
   * @param item stream element (<i>e</i>)
   * @return Pair<isNewItem, itemDropped> where isNewItem is the return value of offer() and
   *     itemDropped is null if no item was dropped
   */
  public Pair<Boolean, T> offerReturnAll(T item, int incrementCount) {
    ListNode2<Counter<T>> counterNode = counterMap.get(item);
    boolean isNewItem = (counterNode == null);
    T droppedItem = null;
    if (isNewItem) {

      if (size() < capacity) {
        counterNode =
            bucketList
                .enqueue(new Bucket(0))
                .getValue()
                .counterList
                .add(new Counter<T>(bucketList.tail(), item));
      } else {
        Bucket min = bucketList.first();
        counterNode = min.counterList.tail();
        Counter<T> counter = counterNode.getValue();
        droppedItem = counter.item;
        counterMap.remove(droppedItem);
        counter.item = item;
        counter.error = min.count;
      }
      counterMap.put(item, counterNode);
    }

    incrementCounter(counterNode, incrementCount);

    return new Pair<Boolean, T>(isNewItem, droppedItem);
  }

  protected void incrementCounter(ListNode2<Counter<T>> counterNode, int incrementCount) {
    Counter<T> counter = counterNode.getValue(); // count_i
    ListNode2<Bucket> oldNode = counter.bucketNode;
    Bucket bucket = oldNode.getValue(); // Let Bucket_i be the bucket of count_i
    bucket.counterList.remove(counterNode); // Detach count_i from Bucket_i's child-list
    counter.count = counter.count + incrementCount;

    // Finding the right bucket for count_i
    // Because we allow a single call to increment count more than once, this may not be the
    // adjacent bucket.
    ListNode2<Bucket> bucketNodePrev = oldNode;
    ListNode2<Bucket> bucketNodeNext = bucketNodePrev.getNext();
    while (bucketNodeNext != null) {
      Bucket bucketNext =
          bucketNodeNext.getValue(); // Let Bucket_i^+ be Bucket_i's neighbor of larger value
      if (counter.count == bucketNext.count) {
        bucketNext.counterList.add(counterNode); // Attach count_i to Bucket_i^+'s child-list
        break;
      } else if (counter.count > bucketNext.count) {
        bucketNodePrev = bucketNodeNext;
        bucketNodeNext = bucketNodePrev.getNext(); // Continue hunting for an appropriate bucket
      } else {
        // A new bucket has to be created
        bucketNodeNext = null;
      }
    }

    if (bucketNodeNext == null) {
      Bucket bucketNext = new Bucket(counter.count);
      bucketNext.counterList.add(counterNode);
      bucketNodeNext = bucketList.addAfter(bucketNodePrev, bucketNext);
    }
    counter.bucketNode = bucketNodeNext;

    // Cleaning up
    if (bucket.counterList.isEmpty()) // If Bucket_i's child-list is empty
    {
      bucketList.remove(oldNode); // Detach Bucket_i from the Stream-Summary
    }
  }

  @Override
  public List<T> peek(int k) {
    List<T> topK = new ArrayList<T>(k);

    for (ListNode2<Bucket> bNode = bucketList.head(); bNode != null; bNode = bNode.getPrev()) {
      Bucket b = bNode.getValue();
      for (Counter<T> c : b.counterList) {
        if (topK.size() == k) {
          return topK;
        }
        topK.add(c.item);
      }
    }

    return topK;
  }

  public List<Counter<T>> topK(int k) {
    List<Counter<T>> topK = new ArrayList<Counter<T>>(k);

    for (ListNode2<Bucket> bNode = bucketList.head(); bNode != null; bNode = bNode.getPrev()) {
      Bucket b = bNode.getValue();
      for (Counter<T> c : b.counterList) {
        if (topK.size() == k) {
          return topK;
        }
        topK.add(c);
      }
    }

    return topK;
  }

  /**
   * @return number of items stored
   */
  public int size() {
    return counterMap.size();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (ListNode2<Bucket> bNode = bucketList.head(); bNode != null; bNode = bNode.getPrev()) {
      Bucket b = bNode.getValue();
      sb.append('{');
      sb.append(b.count);
      sb.append(":[");
      for (Counter<T> c : b.counterList) {
        sb.append('{');
        sb.append(c.item);
        sb.append(':');
        sb.append(c.error);
        sb.append("},");
      }
      if (b.counterList.size() > 0) {
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append("]},");
    }
    if (bucketList.size() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append(']');
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.bucketList = new DoublyLinkedList<Bucket>();
    this.capacity = in.readInt();

    int size = in.readInt();
    this.counterMap = new HashMap<T, ListNode2<Counter<T>>>(size);

    Bucket currentBucket = null;
    ListNode2<Bucket> currentBucketNode = null;
    for (int i = 0; i < size; i++) {
      Counter<T> c = (Counter<T>) in.readObject();
      if (currentBucket == null || c.count != currentBucket.count) {
        currentBucket = new Bucket(c.count);
        currentBucketNode = bucketList.add(currentBucket);
      }
      c.bucketNode = currentBucketNode;
      counterMap.put(c.item, currentBucket.counterList.add(c));
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(this.capacity);
    out.writeInt(this.size());
    for (ListNode2<Bucket> bNode = bucketList.tail(); bNode != null; bNode = bNode.getNext()) {
      Bucket b = bNode.getValue();
      for (Counter<T> c : b.counterList) {
        out.writeObject(c);
      }
    }
  }

  /** For de-serialization */
  public StreamSummary() {}

  /**
   * For de-serialization
   *
   * @param bytes
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public StreamSummary(byte[] bytes) throws IOException, ClassNotFoundException {
    fromBytes(bytes);
  }

  public void fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
    readExternal(new ObjectInputStream(new ByteArrayInputStream(bytes)));
  }

  public byte[] toBytes() throws IOException {
    return ExternalizableUtil.toBytes(this);
  }
}
