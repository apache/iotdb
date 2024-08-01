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
package org.apache.iotdb.commons.schema.filter.impl;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TemplateFilter extends SchemaFilter {
  private final String templateName;
  private final boolean isEqual;

  public TemplateFilter(final String templateName, final boolean isEqual) {
    this.templateName = templateName;
    this.isEqual = isEqual;
  }

  public TemplateFilter(final ByteBuffer byteBuffer) {
    this.templateName = ReadWriteIOUtils.readString(byteBuffer);
    this.isEqual = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public String getTemplateName() {
    return templateName;
  }

  public boolean isEqual() {
    return isEqual;
  }

  @Override
  public <C> Boolean accept(final SchemaFilterVisitor<C> visitor, C node) {
    return visitor.visitTemplateFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.TEMPLATE_FILTER;
  }

  @Override
  protected void serialize(final ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(templateName, byteBuffer);
    ReadWriteIOUtils.write(isEqual, byteBuffer);
  }

  @Override
  protected void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(templateName, stream);
    ReadWriteIOUtils.write(isEqual, stream);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TemplateFilter that = (TemplateFilter) o;
    return Objects.equals(templateName, that.templateName) && Objects.equals(isEqual, that.isEqual);
  }

  @Override
  public int hashCode() {
    return Objects.hash(templateName, isEqual);
  }
}
