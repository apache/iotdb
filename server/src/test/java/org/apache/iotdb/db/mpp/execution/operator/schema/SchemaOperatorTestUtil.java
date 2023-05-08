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
package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.execution.operator.schema.source.ISchemaSource;

import org.mockito.Mockito;

import java.util.Iterator;

public class SchemaOperatorTestUtil {
  public static final String EXCEPTION_MESSAGE = "ExceptionMessage";

  public static <T extends ISchemaInfo> void mockGetSchemaReader(
      ISchemaSource<T> schemaSource,
      Iterator<T> iterator,
      ISchemaRegion schemaRegion,
      boolean isSuccess) {
    Mockito.when(schemaSource.getSchemaReader(schemaRegion))
        .thenReturn(
            new ISchemaReader<T>() {
              @Override
              public boolean isSuccess() {
                return isSuccess;
              }

              @Override
              public Throwable getFailure() {
                return isSuccess ? null : new MetadataException(EXCEPTION_MESSAGE);
              }

              @Override
              public void close() {}

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public T next() {
                return iterator.next();
              }
            });
  }
}
