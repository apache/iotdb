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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import java.util.Objects;

public class PipePatternUtils {
  private PipePatternUtils() {
    // utility class
  }

  public static boolean isExtractAllDevices(
      final TreePattern treePattern, final TablePattern tablePattern) {
    final boolean extractAllTreeDevices = Objects.isNull(treePattern) || (treePattern.isRoot());
    final boolean extractAllTableDevices =
        Objects.isNull(tablePattern)
            || (tablePattern.isTableModelDataAllowedToBeCaptured()
                && !tablePattern.hasUserSpecifiedDatabasePatternOrTablePattern());
    return extractAllTreeDevices && extractAllTableDevices;
  }
}
