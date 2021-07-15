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

package org.apache.iotdb.db.engine.compaction;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class CompactionFuture extends FutureTask<Void> implements Comparable<CompactionFuture> {

  private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss" + ".SSS'Z'");
  private Date createdTime;

  public CompactionFuture(Callable callable) {
    super(callable);
    createdTime = new Date(System.currentTimeMillis());
  }

  public String getCreatedTime() {
    return dateFormat.format(createdTime);
  }

  @Override
  public int compareTo(CompactionFuture future) {
    return this.createdTime.compareTo(future.createdTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompactionFuture future = (CompactionFuture) o;
    return Objects.equals(createdTime, future.createdTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(createdTime);
  }
}
