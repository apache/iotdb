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

package org.apache.iotdb.db.mpp.execution.schedule;

import org.apache.iotdb.db.mpp.execution.driver.IDriver;

/** A common exception to pass to {@link IDriver#failed(Throwable)} */
public class DriverTaskAbortedException extends Exception {

  public static final String BY_TIMEOUT = "timeout";
  public static final String BY_FRAGMENT_ABORT_CALLED = " called";
  public static final String BY_QUERY_CASCADING_ABORTED = "query cascading aborted";
  public static final String BY_ALREADY_BEING_CANCELLED = "already being cancelled";
  public static final String BY_INTERNAL_ERROR_SCHEDULED = "internal error scheduled";

  public DriverTaskAbortedException(String driverTaskName, String causeMsg) {
    super(String.format("DriverTask %s is aborted by %s", driverTaskName, causeMsg));
  }
}
