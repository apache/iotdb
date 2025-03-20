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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;

import java.util.List;

public class OrderedMultiTVListIterator extends MultiTVListIterator {
  public OrderedMultiTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    super(tsDataType, tvLists, deletionList, floatPrecision, encoding);
  }

  @Override
  protected void prepareNext() {
    hasNext = false;
    while (iteratorIndex < tvListIterators.size() - 1
        && !tvListIterators.get(iteratorIndex).hasNextTimeValuePair()) {
      iteratorIndex++;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    if (iterator.hasNextTimeValuePair()) {
      rowIndex = iterator.getIndex();
      hasNext = true;
    }
    probeNext = true;
  }

  @Override
  protected void next() {
    tvListIterators.get(iteratorIndex).next();
    probeNext = false;
  }
}
