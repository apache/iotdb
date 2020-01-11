/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
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

  public RawDataReaderWithoutValueFilter(Path seriesPath, TSDataType dataType,
      Filter filter, QueryContext context, QueryDataSource dataSource)
      throws IOException {
    super(seriesPath, dataType, filter, context, dataSource);
  }

  public RawDataReaderWithoutValueFilter(Path seriesPath, TSDataType dataType,
      Filter filter, QueryContext context, List<TsFileResource> resources)
      throws IOException {
    super(seriesPath, dataType, filter, context, resources);
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
