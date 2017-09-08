package cn.edu.tsinghua.iotdb.engine.overflow.utils;

/**
 * Include three types: INSERT,UPDATE,DELETE;
 *
 * INSERT is an operation which inserts a time point.</br>
 * UPDATE is an operation which updates a time range.</br>
 * DELETE is an operation which deletes a time range. Note that DELETE operation could only
 * delete a time which is less than given time T. </br>
 *
 * @author kangrong
 *
 */
public enum OverflowOpType {
    INSERT, UPDATE, DELETE
}