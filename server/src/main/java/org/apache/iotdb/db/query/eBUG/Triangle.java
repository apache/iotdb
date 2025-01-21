package org.apache.iotdb.db.query.eBUG;

// adapted from the open source C++ code
// https://github.com/ofZach/Visvalingam-Whyatt/blob/master/src/testApp.cpp
public class Triangle {

    int[] indices = new int[3];
    double area;
    Triangle prev;
    Triangle next;
    boolean isDeleted;

    public Triangle(int index1, int index2, int index3, double area) {
        this.indices[0] = index1;
        this.indices[1] = index2;
        this.indices[2] = index3;
        this.area = area;
        this.isDeleted = false; // flag for removal. Avoid using heap.remove(x) as it is O(n) complexity
    }

    public Triangle(Triangle oldTri) {
        // deep copy and inherit connection

        this.indices[0] = oldTri.indices[0];
        this.indices[1] = oldTri.indices[1];
        this.indices[2] = oldTri.indices[2];
        this.area = oldTri.area;
        this.prev = oldTri.prev;
        this.next = oldTri.next;

        // TODO important! inherit connection relationship to this new point
        if (this.prev != null) { // previous point to this new point
            this.prev.next = this;
        }
        if (this.next != null) { // next point to this new point
            this.next.prev = this;
        }

        this.isDeleted = false; // this new triangle is not deleted
    }

    public boolean isValid() {
        return !isDeleted;
    }

    public void markDeleted() {
        this.isDeleted = true;
    }
}
