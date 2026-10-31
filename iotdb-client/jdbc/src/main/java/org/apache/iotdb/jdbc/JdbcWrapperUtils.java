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

package org.apache.iotdb.jdbc;

import org.apache.iotdb.jdbc.i18n.JdbcMessages;

import java.sql.SQLException;

final class JdbcWrapperUtils {

  static boolean isWrapperFor(Object wrapper, Class<?> iface) {
    return iface != null && iface.isInstance(wrapper);
  }

  static <T> T unwrap(Object wrapper, Class<T> iface) throws SQLException {
    if (isWrapperFor(wrapper, iface)) {
      return iface.cast(wrapper);
    }
    throw new SQLException(JdbcMessages.CANNOT_UNWRAP_TO + iface);
  }

  private JdbcWrapperUtils() {}
}
