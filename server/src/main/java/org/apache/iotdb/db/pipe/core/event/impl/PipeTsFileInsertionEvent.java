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

package org.apache.iotdb.db.pipe.core.event.impl;

import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class PipeTsFileInsertionEvent implements TsFileInsertionEvent, EnrichedEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileInsertionEvent.class);
  private File tsFile;

  public PipeTsFileInsertionEvent(File tsFile) {
    this.tsFile = tsFile;
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public TsFileInsertionEvent toTsFileInsertionEvent(Iterable<TabletInsertionEvent> iterable) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String toString() {
    return "PipeTsFileInsertionEvent{" + "tsFile=" + tsFile + '}';
  }

  @Override
  public boolean increaseReferenceCount(String invokerMessage) {
    try {
      this.tsFile = PipeResourceManager.file().increaseFileReference(tsFile, false);
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for TsFile %s error. Invoker Message: %s",
              tsFile.getPath(), invokerMessage),
          e);
      return false;
    }
    return true;
  }

  @Override
  public boolean decreaseReferenceCount(String invokerMessage) {
    try {
      PipeResourceManager.file().decreaseFileReference(tsFile);
    } catch (IOException e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for TsFile %s error. Invoker Message: %s",
              tsFile.getPath(), invokerMessage),
          e);
      return false;
    }
    return true;
  }
}
