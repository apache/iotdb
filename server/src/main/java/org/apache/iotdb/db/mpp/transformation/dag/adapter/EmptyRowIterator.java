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

package org.apache.iotdb.db.mpp.transformation.dag.adapter;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;

import java.io.IOException;

public class EmptyRowIterator implements RowIterator {
  @Override
  public boolean hasNextRow() {
    return false;
  }

  @Override
  public Row next() throws IOException {
    throw new UnsupportedOperationException("Can not call next on EmptyRowIterator");
  }

  @Override
  public void reset() {}
}
