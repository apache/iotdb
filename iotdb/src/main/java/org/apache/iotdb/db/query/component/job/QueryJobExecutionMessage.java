package org.apache.iotdb.db.query.component.job;


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
