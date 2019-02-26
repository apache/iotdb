/**
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
package org.apache.iotdb.db.query.reader.merge;

public class MergeElement implements Comparable<MergeElement> {

  int index;
  long time;
  Object value;
  Integer priority;

  public MergeElement(int index, long time, Object value, int priority) {
    this.index = index;
    this.time = time;
    this.value = value;
    this.priority = priority;
  }

  @Override
  public int compareTo(MergeElement o) {

    if (this.time > o.time) {
      return 1;
    }

    if (this.time < o.time) {
      return -1;
    }

    return o.priority.compareTo(this.priority);
  }

  @Override
  public boolean equals(Object o){
    if (o instanceof MergeElement){
      MergeElement element = (MergeElement) o;
      if (this.time == element.time && this.priority.equals(element.priority)){
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode(){
    return (int) (time * 31 + priority.hashCode());
  }
}
