package cn.edu.tsinghua.iotdb.query.engine.groupby;

/**
 * .
 */
public class GroupByEngineWithFilter {

    /** formNumber is set to -1 default **/
    private int formNumber = -1;

    /** queryFetchSize is sed to read one column data, this variable is mainly used to debug to verify
     * the rightness of iterative readOneColumnWithoutFilter **/
    private int queryFetchSize = 10000;


}
