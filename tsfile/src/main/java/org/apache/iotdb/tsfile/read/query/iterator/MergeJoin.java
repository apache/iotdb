package org.apache.iotdb.tsfile.read.query.iterator;

public class MergeJoin extends BaseJoin {

  private Object[] nextLeft;
  private Object[] nextRight;

  public MergeJoin(TimeSeries left, TimeSeries right) {
    super(left, right);
    nextLeft = left.next();
    nextRight = right.next();
  }

  @Override
  protected boolean setNextIfPossible() {
    if (nextLeft == null & nextRight == null) {
      return false;
    }
    if (nextLeft != null & nextRight == null) {

      Object[] prototype = new Object[nextLeft.length + nextRight.length];
      System.arraycopy(nextLeft, 0, prototype, 0, nextLeft.length);
      System.arraycopy(nextRight, 1, prototype, nextLeft.length, nextRight.length - 1);

      this.nextElement = prototype;
      if (left.hasNext()) {
        nextLeft = left.next();
      } else {
        nextLeft = null;
      }
      return true;
    }
    if (nextLeft == null && nextRight != null) {

      Object[] prototype = new Object[nextLeft.length + nextRight.length];
      prototype[0] = nextRight[0];
      // System.arraycopy(nextLeft, 0, prototype, 0, nextLeft.length);
      System.arraycopy(nextRight, 1, prototype, nextLeft.length, nextRight.length - 1);

      this.nextElement = prototype;
      if (right.hasNext()) {
        nextRight = right.next();
      } else {
        nextRight = null;
      }
      return true;
    }
    if ((long) nextLeft[0] < (long) nextRight[0]) {

      Object[] prototype = new Object[nextLeft.length + nextRight.length];
      System.arraycopy(nextLeft, 0, prototype, 0, nextLeft.length);
//      System.arraycopy(nextRight, 1, prototype, nextLeft.length, nextRight.length - 1);

      if (left.hasNext()) {
        nextLeft = left.next();
      } else {
        nextLeft = null;
      }
    } else if ((long) nextLeft[0] == (long) nextRight[0]) {
      Object[] prototype = new Object[nextLeft.length + nextRight.length];
      System.arraycopy(nextLeft, 0, prototype, 0, nextLeft.length);
      System.arraycopy(nextRight, 1, prototype, nextLeft.length, nextRight.length - 1);
      this.nextElement = prototype;

      // Move both in this case!
      if (left.hasNext()) {
        nextLeft = left.next();
      } else {
        nextLeft = null;
      }
      if (right.hasNext()) {
        nextRight = right.next();
      } else {
        nextRight = null;
      }
    } else {
      Object[] prototype = new Object[nextLeft.length + nextRight.length];
      prototype[0] = nextRight[0];
      // System.arraycopy(nextLeft, 0, prototype, 0, nextLeft.length);
      System.arraycopy(nextRight, 1, prototype, nextLeft.length, nextRight.length - 1);

      this.nextElement = prototype;
      if (right.hasNext()) {
        nextRight = right.next();
      } else {
        nextRight = null;
      }
    }
    return true;
  }
}
