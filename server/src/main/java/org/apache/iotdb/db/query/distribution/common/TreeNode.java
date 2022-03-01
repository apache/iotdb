package org.apache.iotdb.db.query.distribution.common;

import java.util.List;

/**
 * @author A simple class to describe the tree style structure of query executable operators
 * @param <T>
 */
public class TreeNode<T extends TreeNode<T>> {
    protected List<T> children;

    public T getChild(int i) {
        return hasChild(i) ? children.get(i) : null;
    }

    public boolean hasChild(int i) {
        return children.size() > i;
    }

    public void addChild(T n) {
        children.add(n);
    }
}
