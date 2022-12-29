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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.template.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TemplateTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplateTable.class);

  // StorageGroup read write lock
  private final ReentrantReadWriteLock templateReadWriteLock;

  private final AtomicInteger templateIdGenerator;
  private final Map<String, Template> templateMap = new ConcurrentHashMap<>();
  private final Map<Integer, Template> templateIdMap = new ConcurrentHashMap<>();

  private static final String SNAPSHOT_FILENAME = "template_info.bin";

  public TemplateTable() {
    templateReadWriteLock = new ReentrantReadWriteLock();
    templateIdGenerator = new AtomicInteger(0);
  }

  public Template getTemplate(String name) throws MetadataException {
    try {
      templateReadWriteLock.readLock().lock();
      Template template = templateMap.get(name);
      if (template == null) {
        throw new MetadataException(String.format("Template %s does not exist", name));
      }
      return templateMap.get(name);
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
  }

  public Template getTemplate(int templateId) throws MetadataException {
    try {
      templateReadWriteLock.readLock().lock();
      Template template = templateIdMap.get(templateId);
      if (template == null) {
        throw new MetadataException(
            String.format("Template with id=%s does not exist", templateId));
      }
      return template;
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
  }

  public List<Template> getAllTemplate() {
    try {
      templateReadWriteLock.readLock().lock();
      return new ArrayList<>(templateMap.values());
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
  }

  public void createTemplate(Template template) throws MetadataException {
    try {
      templateReadWriteLock.writeLock().lock();
      Template temp = this.templateMap.get(template.getName());
      if (temp != null) {
        LOGGER.error(
            "Failed to create template, because template name {} is exists", template.getName());
        throw new MetadataException("Duplicated template name: " + temp.getName());
      }
      template.setId(templateIdGenerator.getAndIncrement());
      this.templateMap.put(template.getName(), template);
      templateIdMap.put(template.getId(), template);
    } finally {
      templateReadWriteLock.writeLock().unlock();
    }
  }

  public void dropTemplate(String templateName) throws MetadataException {
    try {
      templateReadWriteLock.writeLock().lock();
      Template temp = this.templateMap.remove(templateName);
      if (temp == null) {
        LOGGER.error("Undefined template {}", templateName);
        throw new UndefinedTemplateException(templateName);
      }
      templateIdMap.remove(temp.getId());
    } finally {
      templateReadWriteLock.writeLock().unlock();
    }
  }

  private void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(templateIdGenerator.get(), outputStream);
    ReadWriteIOUtils.write(templateMap.size(), outputStream);
    for (Map.Entry<String, Template> entry : templateMap.entrySet()) {
      serializeTemplate(entry.getValue(), outputStream);
    }
  }

  private void serializeTemplate(Template template, OutputStream outputStream) {
    try {
      template.serialize(outputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void deserialize(InputStream inputStream) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
    templateIdGenerator.set(ReadWriteIOUtils.readInt(byteBuffer));
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    while (size > 0) {
      Template template = deserializeTemplate(byteBuffer);
      templateMap.put(template.getName(), template);
      templateIdMap.put(template.getId(), template);
      size--;
    }
  }

  private Template deserializeTemplate(ByteBuffer byteBuffer) {
    Template template = new Template();
    template.deserialize(byteBuffer);
    return template;
  }

  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "template failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }
    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());
    templateReadWriteLock.writeLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream)) {
      serialize(outputStream);
      outputStream.flush();
      fileOutputStream.flush();
      outputStream.close();
      fileOutputStream.close();
      return tmpFile.renameTo(snapshotFile);
    } finally {
      for (int retry = 0; retry < 5; retry++) {
        if (!tmpFile.exists() || tmpFile.delete()) {
          break;
        } else {
          LOGGER.warn(
              "Can't delete temporary snapshot file: {}, retrying...", tmpFile.getAbsolutePath());
        }
      }
      templateReadWriteLock.writeLock().unlock();
    }
  }

  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    templateReadWriteLock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      // Load snapshot of template
      this.templateMap.clear();
      deserialize(bufferedInputStream);
      bufferedInputStream.close();
      fileInputStream.close();
    } finally {
      templateReadWriteLock.writeLock().unlock();
    }
  }

  @TestOnly
  public void clear() {
    this.templateMap.clear();
  }
}
