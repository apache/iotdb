package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * @Author: LiuDaWei
 * @Create: 2020年01月06日
 */
public interface IRawReader {

  boolean hasNextBatch() throws IOException;

  BatchData nextBatch() throws IOException;

}
