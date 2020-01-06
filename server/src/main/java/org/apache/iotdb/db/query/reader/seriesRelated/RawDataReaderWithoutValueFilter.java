package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * @Author: LiuDaWei
 * @Create: 2020年01月06日
 */
public class RawDataReaderWithoutValueFilter extends AbstractDataReader implements IRawReader {

  public RawDataReaderWithoutValueFilter(Path seriesPath,
      TSDataType dataType,
      Filter filter,
      QueryContext context)
      throws StorageEngineException, IOException {
    super(seriesPath, dataType, filter, context);
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    while (hasNextChunk()) {
      while (hasNextPage()) {
        if (super.hasNextBatch()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return super.nextBatch();
  }
}
