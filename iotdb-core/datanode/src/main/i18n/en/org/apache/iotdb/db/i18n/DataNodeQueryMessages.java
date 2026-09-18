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

package org.apache.iotdb.db.i18n;

public final class DataNodeQueryMessages {

  private DataNodeQueryMessages() {}

  public static final String RESULT_SET_COLUMN_MEMORY_SHORTAGE_EQUIVALENT =
      "The failed memory reservation exceeds available memory by the equivalent of at least "
          + "%,d columns, estimated from the observed average column size. ";

  public static final String
      QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_QUERY_S_THE_CONTEXTHOLDER_546CDD02 =
          "There is not enough memory for Query %s, the contextHolder is %s,current remaining free "
              + "memory is %dB, already reserved memory for this context in total is %dB, the memory "
              + "requested this time is %dB";
}
