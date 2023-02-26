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

package org.apache.iotdb.pipe.api.event.deletion;

import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;

/** DeletionEvent is used to define the event of deletion. */
public interface DeletionEvent extends Event {

  /**
   * The method is used to get the path pattern of the deleted data.
   *
   * @return String
   */
  Path getPath();

  /**
   * The method is used to get the time range of the deleted data.
   *
   * @return TimeRange
   */
  TimeRange getTimeRange();
}
