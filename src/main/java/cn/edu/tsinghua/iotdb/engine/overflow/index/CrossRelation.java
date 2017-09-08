package cn.edu.tsinghua.iotdb.engine.overflow.index;

/**
 * An enum represent the cross relation between two time pairs.
 *
 * @author CGF
 */
public enum CrossRelation {
    LCOVERSR,   // left time pair covers right time pair. e.g. [1, 10], [5, 8]
    RCOVERSL,   // right time pair covers (or equals) left time pair. e.g. [5, 8], [1, 10]
    LFIRSTCROSS, // left time pair first cross right time pair. e.g. [1, 10], [8, 13]
    RFIRSTCROSS, // right time pair first covers left time pair. e.g. [8, 13], [1, 10]
    LFIRST,     // left time pair is on the left of right time pair. e.g. [1, 10], [15, 18]
    RFIRST       // right time pair is on the left of left time pair. e.g. [15, 18], [1, 10]
}
