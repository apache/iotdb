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
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.db.exception.metadata.template.UndefinedTemplateException;
import org.apache.iotdb.db.schemaengine.template.alter.TemplateExtendInfo;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.IOUtils;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
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

import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

public class TemplateTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplateTable.class);

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
    templateReadWriteLock.readLock().lock();
    try {
      Template template = templateMap.get(name);
      if (template == null) {
        throw new MetadataException(
            String.format(ConfigNodeMessages.TEMPLATE_DOES_NOT_EXIST, name));
      }
      return templateMap.get(name);
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
  }

  public Template getTemplate(int templateId) throws MetadataException {
    templateReadWriteLock.readLock().lock();
    try {
      Template template = templateIdMap.get(templateId);
      if (template == null) {
        throw new MetadataException(
            String.format(ConfigNodeMessages.TEMPLATE_WITH_ID_DOES_NOT_EXIST, templateId));
      }
      return template;
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
  }

  public List<Template> getAllTemplate() {
    templateReadWriteLock.readLock().lock();
    try {
      return new ArrayList<>(templateMap.values());
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
  }

  public void createTemplate(final Template template) throws MetadataException {
    templateReadWriteLock.writeLock().lock();
    try {
      final Template temp = this.templateMap.get(template.getName());
      if (temp != null) {
        LOGGER.error(
            ConfigNodeMessages.FAILED_TO_CREATE_TEMPLATE_BECAUSE_TEMPLATE_NAME_EXISTS,
            template.getName());
        throw new MetadataException(ConfigNodeMessages.DUPLICATED_TEMPLATE_NAME + temp.getName());
      }
      template.setId(templateIdGenerator.getAndIncrement());
      this.templateMap.put(template.getName(), template);
      templateIdMap.put(template.getId(), template);
    } finally {
      templateReadWriteLock.writeLock().unlock();
    }
  }

  public void dropTemplate(String templateName) throws MetadataException {
    templateReadWriteLock.writeLock().lock();
    try {
      Template temp = this.templateMap.remove(templateName);
      if (temp == null) {
        LOGGER.error(ConfigNodeMessages.UNDEFINED_TEMPLATE, templateName);
        throw new UndefinedTemplateException(templateName);
      }
      templateIdMap.remove(temp.getId());
    } finally {
      templateReadWriteLock.writeLock().unlock();
    }
  }

  public void extendTemplate(TemplateExtendInfo templateExtendInfo) throws MetadataException {
    templateReadWriteLock.writeLock().lock();
    try {
      Template template = templateMap.get(templateExtendInfo.getTemplateName());
      List<String> measurementList = templateExtendInfo.getMeasurements();
      List<TSDataType> dataTypeList = templateExtendInfo.getDataTypes();
      List<TSEncoding> encodingList = templateExtendInfo.getEncodings();
      List<CompressionType> compressionTypeList = templateExtendInfo.getCompressors();

      IMeasurementSchema measurementSchema;
      for (int i = 0; i < measurementList.size(); i++) {
        measurementSchema = template.getSchema(measurementList.get(i));
        if (measurementSchema == null) {
          template.addMeasurement(
              measurementList.get(i),
              dataTypeList.get(i),
              encodingList == null ? getDefaultEncoding(dataTypeList.get(i)) : encodingList.get(i),
              compressionTypeList == null
                  ? TSFileDescriptor.getInstance().getConfig().getCompressor(dataTypeList.get(i))
                  : compressionTypeList.get(i));
        } else {
          if (!measurementSchema.getType().equals(dataTypeList.get(i))
              || (encodingList != null
                  && !measurementSchema.getEncodingType().equals(encodingList.get(i)))
              || (compressionTypeList != null
                  && !measurementSchema.getCompressor().equals(compressionTypeList.get(i)))) {
            throw new MetadataException(
                String.format(
                    ConfigNodeMessages
                        .SCHEMA_OF_MEASUREMENT_IS_NOT_COMPATIBLE_WITH_EXISTING_MEASUREMENT_IN,
                    measurementList.get(i),
                    template.getName()));
          }
        }
      }

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

  private void serializeTemplate(Template template, OutputStream outputStream) throws IOException {
    template.serialize(outputStream);
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
          ConfigNodeMessages.TEMPLATE_FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY,
          snapshotFile.getAbsolutePath());
      return false;
    }
    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());
    templateReadWriteLock.writeLock().lock();

    try {
      FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
      BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
      try {
        serialize(outputStream);
      } finally {
        outputStream.flush();
        fileOutputStream.getFD().sync();
        outputStream.close();
      }

      return tmpFile.renameTo(snapshotFile);
    } finally {
      for (int retry = 0; retry < 5; retry++) {
        if (!tmpFile.exists() || tmpFile.delete()) {
          break;
        } else {
          LOGGER.warn(
              ConfigNodeMessages.CAN_T_DELETE_TEMPORARY_SNAPSHOT_FILE_RETRYING,
              tmpFile.getAbsolutePath());
        }
      }
      templateReadWriteLock.writeLock().unlock();
    }
  }

  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          ConfigNodeMessages.FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST_2,
          snapshotFile.getAbsolutePath());
      return;
    }
    templateReadWriteLock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      // Load snapshot of template
      this.templateMap.clear();
      this.templateIdMap.clear();
      deserialize(bufferedInputStream);
    } finally {
      templateReadWriteLock.writeLock().unlock();
    }
  }

  @TestOnly
  public void clear() {
    templateReadWriteLock.writeLock().lock();
    try {
      this.templateMap.clear();
      this.templateIdMap.clear();
      this.templateIdGenerator.set(0);
    } finally {
      templateReadWriteLock.writeLock().unlock();
    }
  }
}
