package cn.edu.tsinghua.iotdb.engine.overflow.utils;

/**
 * Used for IntervalTreeOperation.queryMemory() and IntervalTreeOperation.queryFileBlock(); </br>
 *
 * DONE means that a time pair is not used or this time pair has been merged into a new DynamicOneColumn</br>
 * MERGING means that a time pair is merging into a new DynamicOneColumn</br>
 *
 * @author CGF.
 */
public enum  MergeStatus {
    DONE, MERGING
}
