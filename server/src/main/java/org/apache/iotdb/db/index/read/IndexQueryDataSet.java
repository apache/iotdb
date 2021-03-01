package org.apache.iotdb.db.index.read;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;
import java.util.Map;

public class IndexQueryDataSet extends ListDataSet {

  private Map<String, Integer> pathToIndex;

  public IndexQueryDataSet(
      List<PartialPath> paths, List<TSDataType> dataTypes, Map<String, Integer> pathToIndex) {
    super(paths, dataTypes);
    this.pathToIndex = pathToIndex;
  }

  public Map<String, Integer> getPathToIndex() {
    return pathToIndex;
  }
}
