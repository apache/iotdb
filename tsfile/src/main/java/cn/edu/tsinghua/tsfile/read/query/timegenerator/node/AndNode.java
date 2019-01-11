package cn.edu.tsinghua.tsfile.read.query.timegenerator.node;

import java.io.IOException;


public class AndNode implements Node {

    private Node leftChild;
    private Node rightChild;

    private long cachedValue;
    private boolean hasCachedValue;

    public AndNode(Node leftChild, Node rightChild) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.hasCachedValue = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedValue) {
            return true;
        }
        if (leftChild.hasNext() && rightChild.hasNext()) {
            long leftValue = leftChild.next();
            long rightValue = rightChild.next();
            while (true) {
                if (leftValue == rightValue) {
                    this.hasCachedValue = true;
                    this.cachedValue = leftValue;
                    return true;
                } else if (leftValue > rightValue) {
                    if (rightChild.hasNext()) {
                        rightValue = rightChild.next();
                    } else {
                        return false;
                    }
                } else { //leftValue < rightValue
                    if (leftChild.hasNext()) {
                        leftValue = leftChild.next();
                    } else {
                        return false;
                    }
                }
            }
        }
        return false;
    }

    /**
     * If there is no value in current Node, -1 will be returned if {@code next()} is invoked
     */
    @Override
    public long next() throws IOException {
        if (hasNext()) {
            hasCachedValue = false;
            return cachedValue;
        }
        return -1;
    }

    @Override
    public NodeType getType() {
        return NodeType.AND;
    }
}
