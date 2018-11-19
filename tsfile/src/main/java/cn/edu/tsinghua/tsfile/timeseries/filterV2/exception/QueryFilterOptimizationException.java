package cn.edu.tsinghua.tsfile.timeseries.filterV2.exception;

/**
 * Created by zhangjinrui on 2017/12/19.
 */
public class QueryFilterOptimizationException extends Exception{

    public QueryFilterOptimizationException(String msg){
        super(msg);
    }

    public QueryFilterOptimizationException(Throwable cause){
        super(cause);
    }

    public QueryFilterOptimizationException(String message, Throwable cause) {
        super(message, cause);
    }
}
