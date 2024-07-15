package org.apache.iotdb.db.query.simpiece;

public class VisvalPoint {

  public double x, y;
  public VisvalPoint prev, next; // 双向链表中的前驱和后继
  public double area; // 与相邻点形成的三角形面积

  VisvalPoint(double x, double y) {
    this.x = x;
    this.y = y;
  }
}
