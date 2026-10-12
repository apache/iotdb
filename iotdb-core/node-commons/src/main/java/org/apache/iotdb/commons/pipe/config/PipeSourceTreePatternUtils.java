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

package org.apache.iotdb.commons.pipe.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PipeSourceTreePatternUtils {

  private PipeSourceTreePatternUtils() {
    // Utility class.
  }

  /** Splits comma-separated patterns without splitting commas inside backquoted path nodes. */
  public static List<String> splitPatternList(final String patternList) {
    if (patternList == null || patternList.trim().isEmpty()) {
      return Collections.emptyList();
    }
    final List<String> segments = new ArrayList<>();
    final StringBuilder current = new StringBuilder();
    boolean inBackticks = false;
    for (final char c : patternList.toCharArray()) {
      if (c == '`') {
        inBackticks = !inBackticks;
        current.append(c);
      } else if (c == ',' && !inBackticks) {
        final String singlePattern = current.toString().trim();
        if (!singlePattern.isEmpty()) {
          segments.add(singlePattern);
        }
        current.setLength(0);
      } else {
        current.append(c);
      }
    }
    final String lastPattern = current.toString().trim();
    if (!lastPattern.isEmpty()) {
      segments.add(lastPattern);
    }
    return segments;
  }
}
