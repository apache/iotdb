package org.apache.iotdb.pipe.external.kafka;

public class ConsumerSyncStatus {
  private Long numOfSuccessfulInsertion;
  private Long numOfSuccessfulStringInsertion;
  private Long numOfSuccessfulRecordDeletion;
  //  private Long numOfSuccessfulTimeSeriesDeletion;
  private Long numOfFailedInsertion;
  private Long numOfFailedStringInsertion;
  private Long numOfFailedRecordDeletion;
  //  private Long numOfFailedTimeSeriesDeletion;

  ConsumerSyncStatus() {
    numOfSuccessfulInsertion = 0L;
    numOfSuccessfulStringInsertion = 0L;
    numOfSuccessfulRecordDeletion = 0L;
    //    numOfSuccessfulTimeSeriesDeletion = 0L;
    numOfFailedInsertion = 0L;
    numOfFailedStringInsertion = 0L;
    numOfFailedRecordDeletion = 0L;
    //    numOfFailedTimeSeriesDeletion = 0L;
  }

  public void setNumOfSuccessfulInsertion(Long numOfSuccessfulInsertion) {
    this.numOfSuccessfulInsertion = numOfSuccessfulInsertion;
  }

  public Long getNumOfSuccessfulInsertion() {
    return numOfSuccessfulInsertion;
  }

  public void setNumOfSuccessfulStringInsertion(Long numOfSuccessfulStringInsertion) {
    this.numOfSuccessfulStringInsertion = numOfSuccessfulStringInsertion;
  }

  public Long getNumOfSuccessfulStringInsertion() {
    return numOfSuccessfulStringInsertion;
  }

  public void setNumOfSuccessfulRecordDeletion(Long numOfSuccessfulRecordDeletion) {
    this.numOfSuccessfulRecordDeletion = numOfSuccessfulRecordDeletion;
  }

  public Long getNumOfSuccessfulRecordDeletion() {
    return numOfSuccessfulRecordDeletion;
  }

  //  public void setNumOfSuccessfulTimeSeriesDeletion(Long numOfSuccessfulTimeSeriesDeletion) {
  //    this.numOfSuccessfulTimeSeriesDeletion = numOfSuccessfulTimeSeriesDeletion;
  //  }
  //
  //  public Long getNumOfSuccessfulTimeSeriesDeletion() {
  //    return numOfSuccessfulTimeSeriesDeletion;
  //  }

  public void setNumOfFailedInsertion(Long numOfFailedInsertion) {
    this.numOfFailedInsertion = numOfFailedInsertion;
  }

  public Long getNumOfFailedInsertion() {
    return numOfFailedInsertion;
  }

  public void setNumOfFailedStringInsertion(Long numOfFailedStringInsertion) {
    this.numOfFailedStringInsertion = numOfFailedStringInsertion;
  }

  public Long getNumOfFailedStringInsertion() {
    return numOfFailedStringInsertion;
  }

  public void setNumOfFailedRecordDeletion(Long numOfFailedRecordDeletion) {
    this.numOfFailedRecordDeletion = numOfFailedRecordDeletion;
  }

  public Long getNumOfFailedRecordDeletion() {
    return numOfFailedRecordDeletion;
  }

  //  public void setNumOfFailedTimeSeriesDeletion(Long numOfFailedTimeSeriesDeletion) {
  //    this.numOfFailedTimeSeriesDeletion = numOfFailedTimeSeriesDeletion;
  //  }

  //  public Long getNumOfFailedTimeSeriesDeletion() {
  //    return numOfFailedTimeSeriesDeletion;
  //  }
}
