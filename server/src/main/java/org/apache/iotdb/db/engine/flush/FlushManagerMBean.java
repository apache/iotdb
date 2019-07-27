package org.apache.iotdb.db.engine.flush;

public interface FlushManagerMBean {

  public int getNumberOfWorkingTasks();

  public int getNumberOfPendingTasks();

  public int getNumberOfWorkingSubTasks();

  public int getNumberOfPendingSubTasks();

}
