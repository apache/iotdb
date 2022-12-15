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
package org.apache.iotdb.db.mpp.execution.schedule.queue;

/**
 * A simple interface for id getter and setter.
 *
 * <p>Anyone who implements this should implement the code of {@link Object#hashCode()} and {@link
 * Object#equals(Object)} as follows:
 *
 * <pre>
 *   public class T implements IDIndexedAccessible {
 *     private ID id;
 *     ...
 *     public int hashCode() {
 *       return id.hashCode();
 *     }
 *
 *     public boolean equals(Object o) {
 *       return o instanceof T && ((T)o).id.equals(this.id);
 *     }
 *   }
 * </pre>
 *
 * If not, there will be unexpected behaviors using {@link IndexedBlockingQueue}.
 */
public interface IDIndexedAccessible {

  ID getDriverTaskId();

  void setId(ID id);
}
