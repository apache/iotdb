package org.apache.iotdb.db.query.eBUG;

import java.util.ArrayList;
import java.util.List;

public class Polyline {

    private List<Point> vertices = new ArrayList<>();

    public void addVertex(Point point) {
        vertices.add(point);
    }

    public List<Point> getVertices() {
        return new ArrayList<>(vertices);
    }

    public int size() {
        return vertices.size();
    }

    public Point get(int index) {
        return vertices.get(index);
    }
}
