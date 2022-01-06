package org.apache.iotdb.tsfile.read.query.iterator;

public class LeftJoin extends BaseJoin {

  private Object[] nextLeft;
  private Object[] nextRight;

  public LeftJoin(TimeSeries left, TimeSeries right) {
    super(left, right);
    nextLeft = left.next();
    nextRight = right.next();
  }

  @Override
  protected boolean setNextIfPossible() {
    while (true) {
      if (nextRight == null || (long) nextLeft[0] < (long) nextRight[0]) {
        // only return the left one
        this.nextElement = merge();

        return moveLeft();
      } else if ((long) nextLeft[0] == (long) nextRight[0]) {
        // Yay, we have a match
        this.nextElement = merge();

        return moveLeft();
      } else {
        if (right.hasNext()) {
          nextRight = right.next();
        } else {
          nextRight = null;
        }
      }
    }
  }

  private Object[] merge() {
    Object[] prototype = new Object[nextLeft.length + nextRight.length];
    System.arraycopy(nextLeft, 0, prototype, 0, nextLeft.length);
    System.arraycopy(nextRight, 1, prototype, nextLeft.length, nextRight.length - 1);
    return prototype;
  }

  private boolean moveLeft() {
    // Now move the left one forward
    if (left.hasNext()) {
      nextLeft = left.next();
    } else {
      end = true;
    }
    return true;
  }
}
