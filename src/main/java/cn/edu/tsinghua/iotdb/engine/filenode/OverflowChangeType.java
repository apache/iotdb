package cn.edu.tsinghua.iotdb.engine.filenode;

/**
 * if a file is not changed by overflow, it's in NO_CHANGE;<br>
 * if it's changed and in NO_CHANGE previously, NO_CHANGE-->CHANGED, update file<br>
 * If it's changed and in CHANGED previously, and in merging, CHANGED-->MERGING_CHANGE, update
 * file<br>
 * If it's changed and in CHANGED previously, and not in merging, do nothing<br>
 * After merging, if it's MERGING_CHANGE, MERGING_CHANGE-->CHANGED, otherwise in NO_CHANGE,
 * MERGING_CHANGE-->NO_CHANGE
 * 
 * @author kangrong
 *
 */
public enum OverflowChangeType {
    NO_CHANGE, CHANGED, MERGING_CHANGE,
}