package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

public class QueryDataSourceV2 {
  private Path seriesPath;
  private List<TsFileResourceV2> seqResources;
  private List<TsFileResourceV2> unseqResources;
  public QueryDataSourceV2(Path seriesPath, List<TsFileResourceV2> seqResources,List<TsFileResourceV2> unseqResources) {
    this.seriesPath =seriesPath;
    this.seqResources= seqResources;
    this.unseqResources = unseqResources;
  }

  public Path getSeriesPath() {
    return seriesPath;
  }

  public List<TsFileResourceV2> getSeqResources() {
    return seqResources;
  }

  public List<TsFileResourceV2> getUnseqResources() {
    return unseqResources;
  }
}
