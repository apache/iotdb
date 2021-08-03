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

package org.apache.iotdb.db.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * this class can just guarantee some behavior in a concurrent thread safety mode:
 *
 * @param <T>
 */
public class CopyOnReadLinkedList<T> {

  LinkedList<T> data = new LinkedList<>();
  List<T> readCopy;

  public synchronized void add(T d) {
    data.add(d);
  }

  public synchronized boolean contains(T d) {
    return data.contains(d);
  }

  public synchronized void remove(T d) {
    data.remove(d);
  }

  public synchronized Iterator<T> iterator() {
    readCopy = new ArrayList<>(data);
    return readCopy.iterator();
  }

  public synchronized void reset() {
    readCopy = null;
  }

  public synchronized List<T> cloneList() {
    if (readCopy == null) {
      readCopy = new ArrayList<>(data);
    }
    return readCopy;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int size() {
    return data.size();
  }
}
