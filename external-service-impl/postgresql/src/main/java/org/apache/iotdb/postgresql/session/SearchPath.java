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

package org.apache.iotdb.postgresql.session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/** The {@code search_path} setting as a list of schema (database) names. */
public final class SearchPath {

  private SearchPath() {}

  /**
   * The folded names of a {@code search_path} value such as {@code "$user", public, mydb}; {@code
   * $user} is left out (users have no schema of their own). Empty for no value.
   */
  public static List<String> parse(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> names = new ArrayList<>();
    for (String part : value.split(",")) {
      String name = part.trim();
      if (name.length() >= 2 && name.startsWith("\"") && name.endsWith("\"")) {
        name = name.substring(1, name.length() - 1).replace("\"\"", "\"");
      } else if (name.length() >= 2 && name.startsWith("'") && name.endsWith("'")) {
        name = name.substring(1, name.length() - 1);
      } else {
        name = name.toLowerCase(Locale.ROOT);
      }
      if (!name.isEmpty() && !name.equals("$user")) {
        names.add(name);
      }
    }
    return names;
  }
}
