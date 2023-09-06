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

package org.apache.iotdb.db.queryengine.execution.load;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileSplitterTest extends TestBase {
  private List<TsFileData> resultSet = new ArrayList<>();

  @Test
  public void testSplit() throws IOException {
    long start = System.currentTimeMillis();
    int splitId = 0;
    for (File file : files) {
      TsFileSplitter splitter = new TsFileSplitter(file, this::consumeSplit, splitId + 1);
      splitter.splitTsFileByDataPartition();
      splitId = splitter.getCurrentSplitId();
    }
    for (TsFileData tsFileData : resultSet) {
      // System.out.println(tsFileData);
    }
    System.out.printf(
        "%d/%d splits after %dms\n", resultSet.size(), expectedChunkNum(), System.currentTimeMillis() - start);
    assertEquals(resultSet.size(), expectedChunkNum());
  }

  public boolean consumeSplit(TsFileData data) {
    resultSet.add(data);
    return true;
  }
}
