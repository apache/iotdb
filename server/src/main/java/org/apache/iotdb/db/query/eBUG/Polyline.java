package org.apache.iotdb.db.query.eBUG;

import java.util.ArrayList;
import java.util.List;

public class Polyline {

    private List<Point> vertices = new ArrayList<>();

    public void addVertex(Point point) {
        //        if (!vertices.isEmpty()) { //before adding this point
        //            vertices.get(vertices.size() - 1).next = point;
        //            point.prev = vertices.get(vertices.size() - 1);
        //        }
        vertices.add(point);
    }

    public List<Point> getVertices() {
        //        return new ArrayList<>(vertices);
        return vertices;
    }

    public void setVertices(List<Point> points) {
        this.vertices = points;
    }

    public int size() {
        return vertices.size();
    }

    public Point get(int index) {
        return vertices.get(index);
    }

    public void clear() {
        vertices.clear();
    }
}
