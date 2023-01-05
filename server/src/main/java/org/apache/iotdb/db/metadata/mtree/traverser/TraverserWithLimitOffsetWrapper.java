package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.NoSuchElementException;

public class TraverserWithLimitOffsetWrapper<R> extends Traverser<R> {
  private final Traverser<R> traverser;
  private final int limit;
  private final int offset;
  private final boolean hasLimit;

  private int count = 0;
  int curOffset = 0;

  public TraverserWithLimitOffsetWrapper(Traverser<R> traverser, int limit, int offset) {
    this.traverser = traverser;
    this.limit = limit;
    this.offset = offset;
    hasLimit = limit > 0 || offset > 0;

    if (hasLimit) {
      while (curOffset < offset && traverser.hasNext()) {
        traverser.next();
        curOffset++;
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (hasLimit) {
      return count < limit && traverser.hasNext();
    } else {
      return traverser.hasNext();
    }
  }

  @Override
  public R next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    R result = traverser.next();
    if (hasLimit) {
      count++;
    }
    return result;
  }

  @Override
  public void traverse() throws MetadataException {
    traverser.traverse();
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(IMNode node) {
    return false;
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(IMNode node) {
    return false;
  }

  @Override
  protected boolean acceptInternalMatchedNode(IMNode node) {
    return false;
  }

  @Override
  protected boolean acceptFullMatchedNode(IMNode node) {
    return false;
  }

  @Override
  protected R generateResult(IMNode nextMatchedNode) {
    return null;
  }

  @Override
  public void close() {
    traverser.close();
  }

  @Override
  public void reset() {
    traverser.reset();
    count = 0;
    curOffset = 0;
    if (hasLimit) {
      while (curOffset < offset && traverser.hasNext()) {
        traverser.next();
        curOffset++;
      }
    }
  }

  public int getNextOffset() {
    return curOffset + count;
  }
}
