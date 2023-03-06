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
package org.apache.iotdb.db.metadata.newnode;

import org.apache.iotdb.db.metadata.newnode.measurement.MeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateMNodeGenerator;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.Iterator;

public class MemTemplateMNodeGenerator implements TemplateMNodeGenerator<IMemMNode> {

  @Override
  public IMemMNode getChild(Template template, String name) {
    IMeasurementSchema schema = template.getSchema(name);
    return schema == null ? null : new MeasurementMNode(null, name, template.getSchema(name), null);
  }

  @Override
  public Iterator<IMemMNode> getChildren(Template template) {
    return new Iterator<IMemMNode>() {
      private final Iterator<IMeasurementSchema> schemas =
          template.getSchemaMap().values().iterator();

      @Override
      public boolean hasNext() {
        return schemas.hasNext();
      }

      @Override
      public IMemMNode next() {
        IMeasurementSchema schema = schemas.next();
        return new MeasurementMNode(null, schema.getMeasurementId(), schema, null);
      }
    };
  }
}
