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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match;

public class MatchConfig {
  // if the gap between new two points is larger than this times gap before, the new point
  // will consider as a new segment start
  public static final int GAP_TOLERANCE = 2;

  // if the point num more than this while it's heightBound is in smoothValue, it will be
  // considered as a line section
  public static final int LINE_SECTION_TOLERANCE = 4;

  public static final int SHAPE_TOLERANCE = 0;

  public static final boolean CALC_SE_USING_MORE_MEMORY = true;
}
