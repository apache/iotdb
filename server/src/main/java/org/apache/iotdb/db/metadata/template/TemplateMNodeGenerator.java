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
package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.Iterator;

// TODO: it will be generic later
public class TemplateMNodeGenerator {
  public static IMNode getChild(Template template, String name) {
    IMeasurementSchema schema = template.getSchema(name);
    return schema == null ? null : new MeasurementMNode(null, name, template.getSchema(name), null);
  }

  public static Iterator<IMNode> getChildren(Template template) {
    return new Iterator<IMNode>() {
      private final Iterator<IMeasurementSchema> schemas =
          template.getSchemaMap().values().iterator();

      @Override
      public boolean hasNext() {
        return schemas.hasNext();
      }

      @Override
      public IMNode next() {
        IMeasurementSchema schema = schemas.next();
        return new MeasurementMNode(null, schema.getMeasurementId(), schema, null);
      }
    };
  }
}
