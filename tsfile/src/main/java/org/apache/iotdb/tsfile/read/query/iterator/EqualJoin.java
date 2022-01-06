package org.apache.iotdb.tsfile.read.query.iterator;

public class EqualJoin extends BaseJoin {

  private Object[] nextLeft;
  private Object[] nextRight;

  public EqualJoin(TimeSeries left, TimeSeries right) {
    super(left, right);
    nextLeft = left.next();
    nextRight = right.next();
  }

  @Override
  public boolean setNextIfPossible() {
    while (true) {
      if ((long) nextLeft[0] == (long) nextRight[0]) {
        // Yay, we have a match
        this.nextElement = new Object[]{nextLeft[0], nextLeft[1], nextRight[1]};

        // Only increase the one with lower timestamp
        // work with it... increase both
        if (!left.canPeek() || !right.canPeek()) {
          this.end = true;
        } else {
          Object[] peekLeft = left.peek();
          Object[] peekRight = right.peek();

          if ((long) peekLeft[0] <= (long) peekRight[0]) {
            if (left.hasNext()) {
              nextLeft = left.next();
            } else {
              this.end = true;
            }
          } else {
            if (right.hasNext()) {
              nextRight = right.next();
            } else {
              this.end = true;
            }
          }
        }
        return true;
      }
      if ((long) nextLeft[0] > (long) nextRight[0]) {
        if (!right.hasNext()) {
          return false;
        }
        nextRight = right.next();
      } else {
        if (!left.hasNext()) {
          return false;
        }
        nextLeft = left.next();
      }
    }
  }

}
