package org.apache.iotdb.db.query.eBUG;

public class Point {

    double x, y, z;

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
        this.z = Double.POSITIVE_INFINITY; // effective area
    }

    @Override
    public String toString() {
        return "Point: (" + x + ", " + y + ", " + z + ")";
    }
}
