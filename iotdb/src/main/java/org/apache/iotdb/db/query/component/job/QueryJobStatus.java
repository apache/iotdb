package org.apache.iotdb.db.query.component.job;


public enum QueryJobStatus {
    PENDING, READY, RUNNING, FINISHED, WAITING_TO_BE_TERMINATED, TERMINATED
}
