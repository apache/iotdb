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

package org.apache.iotdb.commons.schema.filter.impl.multichildren;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class AbstractMultiChildrenFilter extends SchemaFilter {

  private final List<SchemaFilter> children;

  protected AbstractMultiChildrenFilter(final List<SchemaFilter> children) {
    this.children = children;
  }

  protected AbstractMultiChildrenFilter(final ByteBuffer byteBuffer) {
    final int num = ReadWriteIOUtils.readInt(byteBuffer);
    children = new ArrayList<>();
    for (int i = 0; i < num; ++i) {
      this.children.add(SchemaFilter.deserialize(byteBuffer));
    }
  }

  public List<SchemaFilter> getChildren() {
    return children;
  }

  @Override
  protected void serialize(final ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(children.size(), byteBuffer);
    for (final SchemaFilter child : children) {
      SchemaFilter.serialize(child, byteBuffer);
    }
  }

  @Override
  protected void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(children.size(), stream);
    for (final SchemaFilter child : children) {
      SchemaFilter.serialize(child, stream);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AbstractMultiChildrenFilter that = (AbstractMultiChildrenFilter) o;
    return Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(children);
  }
}
