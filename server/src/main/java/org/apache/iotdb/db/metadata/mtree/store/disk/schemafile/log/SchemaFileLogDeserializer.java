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

package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.log;

import org.apache.iotdb.db.metadata.logfile.IDeserializer;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class SchemaFileLogDeserializer implements IDeserializer<ISchemaPage> {

  @Override
  public ISchemaPage deserialize(InputStream inputStream) throws IOException {
    return IDeserializer.super.deserialize(inputStream);
  }

  @Override
  public ISchemaPage deserialize(ByteBuffer buffer) {
    return IDeserializer.super.deserialize(buffer);
  }
}
