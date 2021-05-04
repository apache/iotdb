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

package org.apache.iotdb.db.engine.compaction.heavyhitter.hitters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.compaction.heavyhitter.QueryHeavyHitters;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashMapHitter extends DefaultHitter implements QueryHeavyHitters  {

  private static final Logger logger = LoggerFactory.getLogger(HashMapHitter.class);
  private Map<PartialPath, Integer> counter = new HashMap<>();

  public HashMapHitter(int maxHitterNum) {
    super(maxHitterNum);
  }

  @Override
  public void acceptQuerySeries(PartialPath queryPath) {
    counter.put(queryPath, counter.getOrDefault(queryPath, 0) + 1);
  }

  @Override
  public List<PartialPath> getTopCompactionSeries(PartialPath sgName) throws MetadataException {
    return null;
  }

  /**
   * used to persist query frequency
   *
   * @param outputPath dump file name
   */
  public void dumpMapToFile(File outputPath) {
    try (BufferedWriter csvWriter = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8), 1024)) {
      File parent = outputPath.getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }
      outputPath.createNewFile();
      for (Map.Entry<PartialPath, Integer> entry : counter.entrySet()) {
        String line = entry.getKey().getFullPath() + "," + entry.getValue();
        csvWriter.write(line);
        csvWriter.newLine();
      }
      csvWriter.flush();
    } catch (IOException e) {
      logger.error("dump map frequency failed, error: {}", e.getMessage(), e);
    }
  }
}
