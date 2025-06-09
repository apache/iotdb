package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model;

public class Point {
  public double x;
  public double y;

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public Point(String point) {
    System.out.println("point: " + point);
    point = point.replaceAll("[()]", "").trim();
    System.out.println("point: " + point);
    String[] xy = point.split(",");
    this.x = Double.parseDouble(xy[0]);
    this.y = Double.parseDouble(xy[1]);
  }

  public void setXY(double x, double y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public String toString() {
    return "Point{" + "x=" + x + ", y=" + y + '}';
  }
}
