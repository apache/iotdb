package org.apache.iotdb.db.metadata.schemainfo;

import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;

public class ITimeSeriesSchemaInfo extends ISchemaInfo {

  private final ShowTimeSeriesResult seriesResult;

  public ITimeSeriesSchemaInfo(ShowTimeSeriesResult seriesResult) {
    this.seriesResult = seriesResult;
  }

  public ShowTimeSeriesResult getSeriesResult() {
    return seriesResult;
  }
}
