package org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure;


import java.io.Serializable;

public class CompressedData implements Serializable {
    // Compressed data processed by the compression sorting algorithm
    public byte[] lens;
    public byte[] vals;

    public CompressedData(byte[] v, byte[] l) {
        this.vals = v;
        this.lens = l;
    }

    public CompressedData() {
        lens = new byte[100];
        vals = new byte[100];
    }

    public void rightMoveVals(int len, int beg, int end) {
        // Shift the elements in the vals array from index beg to end to the right by len positions
        // Ensure that there is no overflow when calling this function
        for(int i=end-1; i>=beg; i--) {
            vals[i+len] = vals[i];
        }
    }

    public void leftMoveVals(int len, int beg, int end) {
        // Shift the elements in the vals array from index beg to end to the right by len positions.
        // Ensure that there is no overflow when calling this function.
        for(int i=beg; i<end; i++) {
            vals[i-len] = vals[i];
        }
    }

    public void expandLens() {
        byte[] expanded = new byte[lens.length*2];
        System.arraycopy(lens, 0, expanded, 0, lens.length);
        this.lens = expanded;
    }

    public void expandVals() {
        byte[] expanded = new byte[vals.length*2];
        System.arraycopy(vals, 0, expanded, 0, vals.length);
        this.vals = expanded;
    }

    public void checkExpand(int valsLen, int valNum, int byteNum) {
        if(valsLen+byteNum >= vals.length){
            expandVals();
        }
        if(valNum/4 >= lens.length-1){
            expandLens();
        }
    }
    public int getLen() {
        return vals.length;
    }

    public void reset(int pointNum, int valsLen) {
        // Compress the array and retain the valid information
        byte[] newLen = new byte[(pointNum+1)/4+1];
        System.arraycopy(lens, 0, newLen, 0, newLen.length);
        byte[] newVals = new byte[valsLen+1];
        System.arraycopy(vals, 0, newVals, 0, newVals.length);
        lens = newLen;
        vals = newVals;
    }
}

