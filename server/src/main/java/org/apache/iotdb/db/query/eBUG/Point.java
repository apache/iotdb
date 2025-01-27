package org.apache.iotdb.db.query.eBUG;

public class Point {

  double x, y, z;

  public Point prev; // 指向T'_max{0,k-e}里当前点的前一个点 for eBUG usage
  public Point next; // 指向T'_max{0,k-e}里当前点的后一个点 for eBUG usage
  // 注意Triangle里的prev&next用于最新状态下存在三角形之间的连接关系，
  // 而这里Point的prev&next用于滞后e个淘汰点（也就是最近的e个淘汰点先不施加）状态下的未淘汰点之间的连接关系

  public int index; // for swab usage

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
    this.z = Double.POSITIVE_INFINITY; // effective area

    // for eBUG usage
    //        this.eliminated = false;
    this.prev = null;
    this.next = null;
  }

  public void markEliminated() {
    // to avoid traversing each point between pa to pb,
    // instead only traversing at most e most recently eliminated points lagged
    prev.next = next;
    next.prev = prev;
  }

  @Override
  public String toString() {
    return "Point: (" + x + ", " + y + ", " + z + ")";
  }

  public double getTimestamp() { // used by FSW
    return x;
  }

  public double getValue() { // used by FSW
    return y;
  }
}
