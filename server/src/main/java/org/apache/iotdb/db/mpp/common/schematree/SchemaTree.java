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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SchemaTree {

  private SchemaNode root;

  /**
   * Return all measurement paths for given path pattern and filter the result by slimit and offset.
   *
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   * @return Left: all measurement paths; Right: remaining series offset
   */
  public Pair<List<MeasurementPath>, Integer> searchMeasurementPaths(
      PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    // TODO: (xingtanzjr) we mock some results here to test the whole procedure
    try {
      String[] paths =
          new String[] {
            "root.sg.d1.s1",
            "root.sg.d1.s2",
            "root.sg.d22.s1",
            "root.sg.d22.s2",
            "root.sg.d333.s1",
            "root.sg.d333.s2",
          };

      List<MeasurementPath> result = new ArrayList<>();
      String target = pathPattern.getFullPath();
      StringBuilder noStar = new StringBuilder();
      boolean lastCharIsStar = false;
      for (int i = 0; i < target.length(); i++) {
        char c = target.charAt(i);
        if (c == '*' || (lastCharIsStar && c == '.')) {
          lastCharIsStar = c == '*';
          continue;
        }
        lastCharIsStar = false;
        noStar.append(String.valueOf(c));
      }

      for (String path : paths) {
        if (path.contains(noStar)) {
          result.add(new MeasurementPath(path));
        }
      }
      return new Pair<>(result, 0);

    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
    return new Pair<>(new ArrayList<>(), 0);
  }

  public void serialize(ByteBuffer buffer) throws IOException {
    // TODO
  }

  public void deserialize(ByteBuffer buffer) throws IOException {
    // TODO
  }

  public List<DataPartitionQueryParam> constructDataPartitionQueryParamList() {
    return new ArrayList<>();
  }
}
