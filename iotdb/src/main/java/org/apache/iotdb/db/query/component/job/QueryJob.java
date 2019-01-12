package org.apache.iotdb.db.query.component.job;


import org.apache.iotdb.tsfile.read.expression.QueryExpression;

public abstract class QueryJob {

    private long jobId;
    private long submitTimestamp;
    private long startTimestamp;
    private long endTimestamp;
    private QueryJobStatus status;
    private QueryJobExecutionMessage message;
    private String clientId;

    protected QueryJobType type;


    public QueryJob(long jobId) {
        this.jobId = jobId;
    }

    public QueryJobStatus getStatus() {
        return status;
    }

    public void setStatus(QueryJobStatus status) {
        this.status = status;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(jobId);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryJob && ((QueryJob) o).getJobId() == jobId) {
            return true;
        }
        return false;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getJobId() {
        return jobId;
    }

    public long getSubmitTimestamp() {
        return submitTimestamp;
    }

    public void setSubmitTimestamp(long submitTimestamp) {
        this.submitTimestamp = submitTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public QueryJobExecutionMessage getMessage() {
        return message;
    }

    public void setMessage(QueryJobExecutionMessage message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return String.valueOf(jobId);
    }

    public QueryJobType getType() {
        return type;
    }

    public static class SelectQueryJob extends QueryJob {

        private QueryExpression queryExpression;
        private int fetchSize;

        public SelectQueryJob(long jobId) {
            super(jobId);
            this.type = QueryJobType.SELECT;
        }

        public QueryExpression getQueryExpression() {
            return queryExpression;
        }

        public void setQueryExpression(QueryExpression queryExpression) {
            this.queryExpression = queryExpression;
        }

        public int getFetchSize() {
            return fetchSize;
        }

        public void setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
        }
    }
}
