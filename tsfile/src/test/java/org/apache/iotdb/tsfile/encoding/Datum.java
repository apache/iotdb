package org.apache.iotdb.tsfile.encoding;

class Datum {
    private double val;
    private double[] probs;

    Datum(double value, int components) {
        this.val = value;
        this.probs = new double[components];
        for (int i = 0; i < probs.length; i++) {
            this.probs[i] = 0.0;
        }
    }

    double val(){
        return this.val;
    }

    void setProb(int i, double val) {
        this.probs[i] = val;
    }

    double getProb(int i) {
        return this.probs[i];
    }
}