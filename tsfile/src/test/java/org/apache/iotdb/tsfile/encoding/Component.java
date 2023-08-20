package org.apache.iotdb.tsfile.encoding;

class Component {
    private double weight, mean, stdev;

    Component(double weight, double mean, double stdev) {
        this.weight = weight;
        this.mean = mean;
        this.stdev = stdev;
    }

    double getWeight() {
        return weight;
    }

    double getMean() {
        return mean;
    }

    double getStdev() {
        return stdev;
    }

    void setWeight(double weight) {
        this.weight = weight;
    }

    void setMean(double mean) {
        this.mean = mean;
    }

    void setStdev(double stdev) {
        this.stdev = stdev;
    }
}
