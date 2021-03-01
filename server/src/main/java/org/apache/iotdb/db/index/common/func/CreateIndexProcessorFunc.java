package org.apache.iotdb.db.index.common.func;

import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;

import java.util.Map;

/**
 * Do something without input and output.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a> whose functional method is
 * {@link #act(PartialPath indexSeries, Map indexInfoMap)}.
 */
@FunctionalInterface
public interface CreateIndexProcessorFunc {

  /** Do something. */
  IndexProcessor act(PartialPath indexSeries, Map<IndexType, IndexInfo> indexInfoMap);
}
