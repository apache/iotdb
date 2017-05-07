package cn.edu.thu.tsfiledb.engine.overflow.index;


import static cn.edu.thu.tsfiledb.engine.overflow.index.CrossRelation.*;

import cn.edu.thu.tsfiledb.engine.overflow.utils.TimePair;

/**
 * This class is used to determine the relation between two time pairs.
 *
 * @author CGF
 */

public class IntervalRelation {

    /**
     * To determine the relation between the two time pairs.
     *
     * @param left - left time pair
     * @param right - right time pair
     */
    private static CrossRelation getCrossRelation(TimePair left, TimePair right) {
        if (right.s <= left.s && right.e >= left.e) { // right covers left | right equals left
            return RCOVERSL;
        } else if (right.s >= left.s && right.e <= left.e) {  // left covers right
            return LCOVERSR;
        } else if (right.s > left.s) {    // left first cross
            return LFIRSTCROSS;
        } else {    // right first cross
            return RFIRSTCROSS;
        }
    }

    private static CrossRelation getCrossRelation(long s1, long e1, long s2, long e2) {
        if (s2 <= s1 && e2 >= e1) { // right covers left | right equals left
            return RCOVERSL;
        } else if (s2 >= s1 && e2 <= e1) {  // left covers right
            return LCOVERSR;
        } else if (s2 > s1) {    // left first cross
            return LFIRSTCROSS;
        } else {    // right first cross
            return RFIRSTCROSS;
        }
    }

    /**
     * @param left - left time pair
     * @param right - right time pair
     * @return CrossRelation
     */

    public static CrossRelation getRelation(TimePair left, TimePair right) {
        if (left.e < right.s) {    // left first
            return LFIRST;
        } else if (right.e < left.s) { // right first
            return RFIRST;
        } else
            return getCrossRelation(left, right);
    }

    public static CrossRelation getRelation(long s1, long e1, long s2, long e2) {
        if (e1 < s2) {    // left first
            return LFIRST;
        } else if (e2 < s1) { // right first
            return RFIRST;
        } else
            return getCrossRelation(s1, e1, s2, e2);
    }
}
