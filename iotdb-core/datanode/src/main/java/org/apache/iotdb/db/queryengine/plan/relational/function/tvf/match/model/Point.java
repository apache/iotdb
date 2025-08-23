package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model;

public class Point {
  public double x;
  public double y;
  public int index;

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public Point(double x, double y, int index) {
    this.x = x;
    this.y = y;
    this.index = index;
  }

  @Override
  public String toString() {
    return "Point{" + "x=" + x + ", y=" + y + '}';
  }
}
