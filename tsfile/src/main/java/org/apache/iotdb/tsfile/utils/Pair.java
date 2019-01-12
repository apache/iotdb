package org.apache.iotdb.tsfile.utils;

/**
 * Pair is a template class to represent a couple of values. It also override the Object basic
 * methods like hasnCode, equals and toString.
 *
 * @param <L> L type
 * @param <R> R type
 * @author kangrong
 */
public class Pair<L, R> {
    public L left;
    public R right;

    public Pair(L l, R r) {
        left = l;
        right = r;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((left == null) ? 0 : left.hashCode());
        result = prime * result + ((right == null) ? 0 : right.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Pair<?, ?> other = (Pair<?, ?>) obj;
        if (left == null) {
            if (other.left != null)
                return false;
        } else if (!left.equals(other.left))
            return false;
        if (right == null) {
            if (other.right != null)
                return false;
        } else if (!right.equals(other.right))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "<" + left + "," + right + ">";
    }
}
