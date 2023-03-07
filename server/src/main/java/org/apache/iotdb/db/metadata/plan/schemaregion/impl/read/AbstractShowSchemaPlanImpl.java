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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.read;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowSchemaPlan;

import java.util.Objects;

public abstract class AbstractShowSchemaPlanImpl implements IShowSchemaPlan {

  protected final PartialPath path;
  protected final long limit;
  protected final long offset;
  protected final boolean isPrefixMatch;

  protected AbstractShowSchemaPlanImpl(PartialPath path) {
    this.path = path;
    this.limit = 0;
    this.offset = 0;
    this.isPrefixMatch = false;
  }

  AbstractShowSchemaPlanImpl(PartialPath path, long limit, long offset, boolean isPrefixMatch) {
    this.path = path;
    this.limit = limit;
    this.offset = offset;
    this.isPrefixMatch = isPrefixMatch;
  }

  @Override
  public PartialPath getPath() {
    return path;
  }

  @Override
  public long getLimit() {
    return limit;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public boolean isPrefixMatch() {
    return isPrefixMatch;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AbstractShowSchemaPlanImpl that = (AbstractShowSchemaPlanImpl) o;
    return limit == that.limit
        && offset == that.offset
        && isPrefixMatch == that.isPrefixMatch
        && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, limit, offset, isPrefixMatch);
  }
}
