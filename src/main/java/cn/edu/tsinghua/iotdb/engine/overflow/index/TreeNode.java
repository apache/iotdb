package cn.edu.tsinghua.iotdb.engine.overflow.index;


import java.util.Random;

import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowOpType;

/**
 * Used for IntervalTree (Treap Data Structure).
 *
 * @author CGF
 */

public class TreeNode {

    private Random random = new Random();
    public long start; // start time
    public long end;   // end time
    public int fix;    // priority for treap IntervalTree
    public OverflowOpType opType; // overflow operation type
    public byte[] value;   // the value stored in this node
    public TreeNode left;
    public TreeNode right;

    public TreeNode(long start, long end, byte[] value, OverflowOpType type) {
        this.start = start;
        this.end = end;
        this.value = value;
        this.opType = type;
        left = null;
        right = null;
        fix = random.nextInt();
    }
}