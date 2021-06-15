package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class TsFileResourceList implements List<TsFileResource> {
  private TsFileResource header;
  private TsFileResource tail;
  private int count = 0;

  /**
   * Insert a new node before an existing node
   *
   * @param node the existing node
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
   * @param node the existing node
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
      current = current.next;
    }
    return contain;
  }

  @Override
  public Iterator<TsFileResource> iterator() {
    return new TsFileResourceIterator();
  }

  /** Insert a new tsFileResource node to the end of List */
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

  /** The tsFileResource to be removed must be in the list, otherwise may cause unknown behavior */
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
    throw new NotImplementedException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new NotImplementedException();
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
    throw new NotImplementedException();
  }

  @Override
  public TsFileResource set(int index, TsFileResource element) {
    throw new NotImplementedException();
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
}
