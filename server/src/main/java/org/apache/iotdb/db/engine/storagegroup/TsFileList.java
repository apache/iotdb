package org.apache.iotdb.db.engine.storagegroup;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class TsFileList implements List<TsFileResource> {
  private TsFileResource header;
  private TsFileResource tail;
  private int count = 0;

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
    if (o.getClass() != TsFileResource.class) {
      return false;
    }
    boolean contain = false;
    TsFileResource current = header;
    while (current != null) {
      if (current.equals(o)) {
        contain = true;
        break;
      }
    }
    return contain;
  }

  @Override
  public Iterator<TsFileResource> iterator() {
    return new TsFileResourceIterator();
  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return null;
  }

  /**
   * Insert a new tsFileResource node to the end of List
   */
  @Override
  public boolean add(TsFileResource tsFileResource) {
    if (tail == null) {
      header = tsFileResource;
      tail = tsFileResource;
      tsFileResource.prev = null;
      tsFileResource.next = null;
    } else {
      insertAfter(tail, tsFileResource);
    }
    return true;
  }

  /**
   * The tsFileResource to be removed must be in the list, otherwise may cause unknown behavior
   */
  @Override
  public boolean remove(Object o) {
    TsFileResource tsFileResource = (TsFileResource) o;
    if (tsFileResource.prev == null) {
      header = header.next;
      header.prev = null;
    } else if (tsFileResource.next == null) {
      tail = tail.prev;
      tail.next = null;
    } else {
      tsFileResource.prev.next = tsFileResource.next;
    }
    return true;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends TsFileResource> c) {
    return false;
  }

  @Override
  public boolean addAll(int index, Collection<? extends TsFileResource> c) {
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return false;
  }

  @Override
  public void clear() {

  }

  @Override
  public TsFileResource get(int index) {
    return null;
  }

  @Override
  public TsFileResource set(int index, TsFileResource element) {
    return null;
  }

  @Override
  public void add(int index, TsFileResource element) {

  }

  @Override
  public TsFileResource remove(int index) {
    return null;
  }

  @Override
  public int indexOf(Object o) {
    return 0;
  }

  @Override
  public int lastIndexOf(Object o) {
    return 0;
  }

  @Override
  public ListIterator<TsFileResource> listIterator() {
    return null;
  }

  @Override
  public ListIterator<TsFileResource> listIterator(int index) {
    return null;
  }

  @Override
  public List<TsFileResource> subList(int fromIndex, int toIndex) {
    return null;
  }

  /**
   * Insert a new node before an existing node
   *
   * @param node    the existing node
   * @param newNode the new node to insert
   */
  public void insertBefore(TsFileResource node, TsFileResource newNode) {
    newNode.prev = node.prev;
    newNode.next = node;
    if (node.prev == null) {
      header = newNode;
    } else {
      node.prev.next = newNode;
    }
    node.prev = newNode;
  }

  /**
   * Insert a new node after an existing node
   *
   * @param node    the existing node
   * @param newNode the new node to insert
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
  }

  private class TsFileResourceIterator implements Iterator<TsFileResource> {
    TsFileResource current;

    public TsFileResourceIterator() {
      this.current = header;
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public TsFileResource next() {
      TsFileResource temp = current;
      current = current.next;
      return temp;
    }
  }

  public static void main(String[] args) {
    TsFileList list = new TsFileList();
    for (TsFileResource i : list) {
      i.calculateRamSize();
    }
  }
}
