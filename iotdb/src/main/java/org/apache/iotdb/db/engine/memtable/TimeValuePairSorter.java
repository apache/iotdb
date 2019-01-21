/**
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
package org.apache.iotdb.db.engine.memtable;

import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;

public interface TimeValuePairSorter {

  /**
   * get the distinct sorted startTime.
   *
   * @return a List which contains all distinct {@link TimeValuePair}s in ascending order by
   * timestamp.
   */
  List<TimeValuePair> getSortedTimeValuePairList();

  /**
   * notice, by default implementation, calling this method will cause calling
   * getSortedTimeValuePairList().
   *
   * @return an iterator of data in this class.
   */
  default Iterator<TimeValuePair> getIterator() {
    return getSortedTimeValuePairList().iterator();
  }

  /**
   * notice, by default implementation, calling this method will cause calling
   * getSortedTimeValuePairList().
   *
   * @return if there is no data in this sorter, return true.
   */
  default boolean isEmpty() {
    return getSortedTimeValuePairList().isEmpty();
  }
}
