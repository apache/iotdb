package org.apache.iotdb.tsfile.encoding;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataSet implements Iterable{
    private ArrayList<Datum> data;
    private int components;

    DataSet(List<Double> nums, int components) {
        this.components = components;
        this.data = new ArrayList<>(nums.size());
        nums.forEach(n->this.data.add(new Datum(n, components)));
    }

    double getStdev() {
        double mean = this.getMean();
        double stdev = 0.0;
        for (Datum d : this.data)
            stdev += Math.pow(d.val()-mean,2);

        stdev /= this.data.size();
        stdev = Math.sqrt(stdev);
        return stdev;
    }

    double getMean() {
        double mean = 0.0;
        for (Datum d : this.data )
            mean += d.val();

        mean /= this.data.size();
        return mean;
    }

    int components() {
        return this.components;
    }

    double nI(int i) {
        double sum = 0.0;
        for (Datum d : this.data) {
            sum += d.getProb(i);
        }
        return sum;
    }

    int size(){
        return this.data.size();
    }

    Datum get(int i) {
        return data.get(i);
    }

    public Iterator iterator() {
        return this.data.iterator();
    }
}
