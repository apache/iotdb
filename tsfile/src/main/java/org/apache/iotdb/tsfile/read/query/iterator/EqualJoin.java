package org.apache.iotdb.tsfile.read.query.iterator;

public class EqualJoin extends BaseJoin {

  private Object[] nextLeft;
  private Object[] nextRight;
  private final boolean ascending;

  public EqualJoin(TimeSeries left, TimeSeries right) {
    this(left, right, true);
  }

  public EqualJoin(TimeSeries left, TimeSeries right, boolean ascending) {
    super(left, right);
    nextLeft = left.next();
    nextRight = right.next();
    this.ascending = ascending;
  }

  @Override
  public boolean setNextIfPossible() {
    if (ascending) {
      return setNextIfPossibleAsc();
    } else {
      return setNextIfPossibleDesc();
    }
  }

  public boolean setNextIfPossibleAsc() {
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
          this.nextElement = null;
          return false;
        }
        nextRight = right.next();
      } else {
        if (!left.hasNext()) {
          this.nextElement = null;
          return false;
        }
        nextLeft = left.next();
      }
    }
  }

  public boolean setNextIfPossibleDesc() {
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

          if ((long) peekLeft[0] >= (long) peekRight[0]) {
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
      if ((long) nextLeft[0] < (long) nextRight[0]) {
        if (!right.hasNext()) {
          this.nextElement = null;
          return false;
        }
        nextRight = right.next();
      } else {
        if (!left.hasNext()) {
          this.nextElement = null;
          return false;
        }
        nextLeft = left.next();
      }
    }
  }

}
