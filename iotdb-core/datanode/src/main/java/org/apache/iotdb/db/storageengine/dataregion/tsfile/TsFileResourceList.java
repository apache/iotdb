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

package org.apache.iotdb.db.storageengine.dataregion.tsfile;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.apache.tsfile.exception.NotImplementedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TsFileResourceList implements List<TsFileResource> {

  private TsFileResource header;
  private TsFileResource tail;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private int count = 0;

  /**
   * Insert a new node before an existing node
   *
   * @param node the existing node
   * @param newNode the file to insert
   */
  public void insertBefore(TsFileResource node, TsFileResource newNode) {
    if (newNode.equals(node)) {
      return;
    }
    newNode.prev = node.prev;
    newNode.next = node;
    if (node.prev == null) {
      header = newNode;
    } else {
      node.prev.next = newNode;
    }
    node.prev = newNode;
    count++;
  }

  /**
   * Insert a new node after an existing node
   *
   * @param node the existing node
   * @param newNode the file to insert
   */
  public void insertAfter(TsFileResource node, TsFileResource newNode) {
    newNode.prev = node;
    newNode.next = node.next;
    if (node.next == null) {
      tail = newNode;
    } else {
      node.next.prev = newNode;
    }
    node.next = newNode;
    count++;
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public boolean isEmpty() {
    return count == 0;
  }

  @Override
  public boolean contains(Object o) {
    if (!(o instanceof TsFileResource)) {
      return false;
    }
    boolean contain = false;
    TsFileResource current = header;
    while (current != null) {
      if (current.equals(o)) {
        contain = true;
        break;
      }
      current = current.next;
    }
    return contain;
  }

  @Override
  public Iterator<TsFileResource> iterator() {
    return new TsFileIterator();
  }

  public Iterator<TsFileResource> reverseIterator() {
    return new TsFileReverseIterator();
  }

  /** Insert a new tsFileResource node to the end of List */
  @Override
  public boolean add(TsFileResource newNode) {
    if (newNode.prev != null || newNode.next != null) {
      // this node already in a list
      return false;
    }
    if (tail == null) {
      header = newNode;
      tail = newNode;
      count++;
    } else {
      insertAfter(tail, newNode);
    }
    return true;
  }

  /**
   * Insert a tsfile resource to the list, the tsfile will be inserted before the first tsfile whose
   * timestamp is greater than its. If there is no tsfile whose timestamp is greater than the new
   * node's, the new node will be inserted to the tail of the list.
   */
  public boolean keepOrderInsert(TsFileResource newNode) throws IOException {
    if (newNode.prev != null || newNode.next != null || (count == 1 && header == newNode)) {
      // this node already in a list
      return false;
    }
    if (tail == null) {
      // empty list
      header = newNode;
      tail = newNode;
      count++;
    } else {
      TsFileNameGenerator.TsFileName newTsFileName =
          TsFileNameGenerator.getTsFileName(newNode.getTsFile().getName());
      long timestampOfNewNode = newTsFileName.getTime();
      long versionOfNewNode = newTsFileName.getVersion();

      // find the position to insert, the list should be ordered by file timestamp. If timestamp is
      // equal to each other, then list should be ordered by file version.
      boolean isNewNodeAtTail = true;
      TsFileResource currNode = header;
      while (currNode != null) {
        TsFileNameGenerator.TsFileName currTsFileName =
            TsFileNameGenerator.getTsFileName(currNode.getTsFile().getName());
        if (timestampOfNewNode == currTsFileName.getTime()
            && versionOfNewNode < currTsFileName.getVersion()) {
          // the timestamp of new node is equal to the current node and the version of new node is
          // less than current node, then insert new node before the current node
          isNewNodeAtTail = false;
          break;
        } else if (timestampOfNewNode < currTsFileName.getTime()) {
          // the timestamp of new node is less than the current node, insert new node before then
          // current node
          isNewNodeAtTail = false;
          break;
        }
        currNode = currNode.next;
      }

      // insert new node
      if (isNewNodeAtTail) {
        insertAfter(tail, newNode);
      } else {
        insertBefore(currNode, newNode);
      }
    }
    return true;
  }

  /**
   * The tsFileResourceListNode to be removed must be in the list, otherwise may cause unknown
   * behavior
   */
  @Override
  public boolean remove(Object o) {
    TsFileResource tsFileResource = (TsFileResource) o;
    if (!contains(o)) {
      // the tsFileResource does not exist in this list
      return false;
    }
    if (tsFileResource.prev == null) {
      // remove header
      header = header.next;
      if (header != null) {
        header.prev = null;
      } else {
        // if list contains only one item, remove the header and the tail
        tail = null;
      }
    } else if (tsFileResource.next == null) {
      // remove tail
      tail = tail.prev;
      if (tail != null) {
        tail.next = null;
      } else {
        // if list contains only one item, remove the header and the tail
        header = null;
      }
    } else {
      tsFileResource.prev.next = tsFileResource.next;
      tsFileResource.next.prev = tsFileResource.prev;
    }
    tsFileResource.prev = null;
    tsFileResource.next = null;
    count--;
    return true;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  /** Only List type parameter is legal, because it is in order. */
  @Override
  public boolean addAll(Collection<? extends TsFileResource> c) {
    if (c instanceof List) {
      for (TsFileResource resource : c) {
        add(resource);
      }
      return true;
    }
    throw new NotImplementedException();
  }

  @Override
  public void clear() {
    header = null;
    tail = null;
    count = 0;
  }

  @Override
  public Object[] toArray() {
    return getArrayList().toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return getArrayList().toArray(a);
  }

  @Override
  public boolean addAll(int index, Collection<? extends TsFileResource> c) {
    throw new NotImplementedException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new NotImplementedException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new NotImplementedException();
  }

  @Override
  public TsFileResource get(int index) {
    int currIndex = 0;
    TsFileResource currTsFileResource = header;
    while (currIndex != index) {
      if (currTsFileResource.next == null) {
        throw new ArrayIndexOutOfBoundsException(currIndex);
      } else {
        currTsFileResource = currTsFileResource.next;
      }
      currIndex++;
    }
    return currTsFileResource;
  }

  /**
   * insert tsFileResource to a target pos(targetPos = index) e.g. if index = 0, then to the first,
   * if index = 1, then to the second.
   */
  @Override
  public TsFileResource set(int index, TsFileResource element) {
    int currIndex = 0;
    TsFileResource currTsFileResource = header;
    if (header == null && index > 0) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    while (currIndex != index) {
      if (currTsFileResource != null && currTsFileResource.next == null) {
        if (currIndex == index - 1) {
          insertAfter(currTsFileResource, element);
          return element;
        } else {
          throw new ArrayIndexOutOfBoundsException(currIndex);
        }
      }
      currIndex++;
    }
    if (currTsFileResource != null) {
      insertBefore(currTsFileResource, element);
    } else {
      add(element);
    }
    return element;
  }

  @Override
  public void add(int index, TsFileResource element) {
    throw new NotImplementedException();
  }

  @Override
  public TsFileResource remove(int index) {
    throw new NotImplementedException();
  }

  @Override
  public int indexOf(Object o) {
    throw new NotImplementedException();
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new NotImplementedException();
  }

  @Override
  public ListIterator<TsFileResource> listIterator() {
    throw new NotImplementedException();
  }

  @Override
  public ListIterator<TsFileResource> listIterator(int index) {
    throw new NotImplementedException();
  }

  @Override
  public List<TsFileResource> subList(int fromIndex, int toIndex) {
    throw new NotImplementedException();
  }

  public List<TsFileResource> getArrayList() {
    List<TsFileResource> list = new ArrayList<>(count);
    TsFileResource current = header;
    while (current != null) {
      list.add(current);
      current = current.next;
    }
    return list;
  }

  private class TsFileIterator implements Iterator<TsFileResource> {
    List<TsFileResource> tsFileResourceList;
    int currentIndex = 0;

    public TsFileIterator() {
      this.tsFileResourceList = getArrayList();
    }

    @Override
    public boolean hasNext() {
      return currentIndex < tsFileResourceList.size();
    }

    @Override
    public TsFileResource next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return this.tsFileResourceList.get(currentIndex++);
    }
  }

  private class TsFileReverseIterator implements Iterator<TsFileResource> {
    List<TsFileResource> tsFileResourceList;
    int currentIndex;

    public TsFileReverseIterator() {
      tsFileResourceList = getArrayList();
      currentIndex = tsFileResourceList.size() - 1;
    }

    @Override
    public boolean hasNext() {
      return currentIndex >= 0;
    }

    @Override
    public TsFileResource next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return tsFileResourceList.get(currentIndex--);
    }
  }

  @TestOnly
  public TsFileResource getHeader() {
    return header;
  }

  @TestOnly
  public TsFileResource getTail() {
    return tail;
  }
}
