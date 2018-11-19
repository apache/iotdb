package cn.edu.tsinghua.iotdb.queryV2.engine.component.job;

/**
 * Created by zhangjinrui on 2018/1/10.
 */
public class QueryJobExecutionMessage {

    private String message;

    public QueryJobExecutionMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
