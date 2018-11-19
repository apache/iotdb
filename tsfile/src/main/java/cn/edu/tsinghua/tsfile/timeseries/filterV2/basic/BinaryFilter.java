package cn.edu.tsinghua.tsfile.timeseries.filterV2.basic;


import java.io.Serializable;

/**
 * Definition for binary filter operations.
 *
 * @author CGF
 */

public abstract class BinaryFilter<T extends Comparable<T>> implements Filter<T>, Serializable {

    private static final long serialVersionUID = 1039585564327602465L;

    protected final Filter left;
    protected final Filter right;

    protected BinaryFilter(Filter left, Filter right) {
        this.left = left;
        this.right = right;
    }

    public Filter getLeft() {
        return left;
    }

    public Filter getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "( " + left + "," + right + " )";
    }
}
