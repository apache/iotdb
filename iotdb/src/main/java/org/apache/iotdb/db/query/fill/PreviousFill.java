/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.fill;

import java.io.IOException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

public class PreviousFill extends IFill {

  private long beforeRange;

  private Path path;

  private BatchData result;

  public PreviousFill(Path path, TSDataType dataType, long queryTime, long beforeRange) {
    super(dataType, queryTime);
    this.path = path;
    this.beforeRange = beforeRange;
    result = new BatchData(dataType, true, true);
  }

  public PreviousFill(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  @Override
  public IFill copy(Path path) {
    return new PreviousFill(path, dataType, queryTime, beforeRange);
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  @Override
  public BatchData getFillResult() throws ProcessorException, IOException, PathErrorException {
    long beforeTime;
    if (beforeRange == -1) {
      beforeTime = 0;
    } else {
      beforeTime = queryTime - beforeRange;
    }
    return result;
  }
}
