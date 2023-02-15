package org.apache.iotdb.db.metadata.rescon;

public interface ISchemaRegionStatistics {

  boolean isAllowToCreateNewSeries();

  long getRegionMemoryUsage();

  int getSchemaRegionId();

  ISchemaEngineStatistics getSchemaEngineStatistics();

  void clear();
}
