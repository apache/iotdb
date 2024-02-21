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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.log.SchemaFileLogReader;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.log.SchemaFileLogWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile.getPageAddress;

public class PageIOChannel {
  private final FileChannel channel;
  private final File pmtFile;
  private FileChannel readChannel;
  private final AtomicInteger logCounter;
  private SchemaFileLogWriter logWriter;

  // flush strategy is dependent on consensus protocol, only check protocol on init
  protected FlushPageStrategy flushDirtyPagesStrategy;
  protected SinglePageFlushStrategy singlePageFlushStrategy;

  PageIOChannel(FileChannel channel, File pmtFile, boolean flushWithLogging, String logPath)
      throws IOException, MetadataException {
    this.channel = channel;
    this.pmtFile = pmtFile;
    this.readChannel = FileChannel.open(pmtFile.toPath(), StandardOpenOption.READ);
    if (flushWithLogging) {
      // without RATIS, utilize physical logging for integrity
      int pageAcc = (int) recoverFromLog(logPath) / SchemaFileConfig.PAGE_LENGTH;
      this.logWriter = new SchemaFileLogWriter(logPath);
      logCounter = new AtomicInteger(pageAcc);
      flushDirtyPagesStrategy = this::flushDirtyPagesWithLogging;
      singlePageFlushStrategy = this::flushSinglePageWithLogging;
    } else {
      // with RATIS enabled, integrity is guaranteed by consensus protocol
      logCounter = new AtomicInteger();
      logWriter = null;
      flushDirtyPagesStrategy = this::flushDirtyPagesWithoutLogging;
      singlePageFlushStrategy = this::flushSinglePageWithoutLogging;
    }
  }

  public void renewLogWriter() throws IOException {
    if (logWriter != null) {
      logWriter = logWriter.renew();
    }
  }

  public void closeLogWriter() throws IOException {
    if (logWriter != null) {
      logWriter.close();
    }
  }

  /** Load bytes from log, deserialize and flush directly into channel, return current length */
  private long recoverFromLog(String logPath) throws IOException, MetadataException {
    SchemaFileLogReader reader = new SchemaFileLogReader(logPath);
    ISchemaPage page;
    List<byte[]> res = reader.collectUpdatedEntries();
    for (byte[] entry : res) {
      // TODO check bytes semantic correctness with CRC32 or other way
      page = ISchemaPage.loadSchemaPage(ByteBuffer.wrap(entry));
      page.flushPageToChannel(this.channel);
    }
    reader.close();

    // complete log file
    if (!res.isEmpty()) {
      try (FileOutputStream outputStream = new FileOutputStream(logPath, true)) {
        outputStream.write(new byte[] {SchemaFileConfig.SF_COMMIT_MARK});
        return outputStream.getChannel().size();
      }
    }
    return 0L;
  }

  public void loadFromFileToBuffer(ByteBuffer dst, int pageIndex) throws IOException {
    dst.clear();
    if (!readChannel.isOpen()) {
      readChannel = FileChannel.open(pmtFile.toPath(), StandardOpenOption.READ);
    }
    readChannel.read(dst, getPageAddress(pageIndex));
  }

  // region Flush Strategy
  @FunctionalInterface
  interface FlushPageStrategy {
    void apply(List<ISchemaPage> dirtyPages) throws IOException;
  }

  @FunctionalInterface
  interface SinglePageFlushStrategy {
    void apply(ISchemaPage page) throws IOException;
  }

  private void flushDirtyPagesWithLogging(List<ISchemaPage> dirtyPages) throws IOException {
    if (dirtyPages.size() == 0) {
      return;
    }

    if (logCounter.get() > SchemaFileConfig.SCHEMA_FILE_LOG_SIZE) {
      logWriter = logWriter.renew();
      logCounter.set(0);
    }

    logCounter.addAndGet(dirtyPages.size());
    for (ISchemaPage page : dirtyPages) {
      page.syncPageBuffer();
      logWriter.write(page);
    }
    logWriter.prepare();

    for (ISchemaPage page : dirtyPages) {
      page.flushPageToChannel(channel);
    }
    logWriter.commit();
  }

  private void flushSinglePageWithLogging(ISchemaPage page) throws IOException {
    if (logCounter.get() > SchemaFileConfig.SCHEMA_FILE_LOG_SIZE) {
      logWriter = logWriter.renew();
      logCounter.set(0);
    }

    logCounter.addAndGet(1);
    page.syncPageBuffer();
    logWriter.write(page);
    logWriter.prepare();
    page.flushPageToChannel(channel);
    logWriter.commit();
  }

  private void flushDirtyPagesWithoutLogging(List<ISchemaPage> dirtyPages) throws IOException {
    for (ISchemaPage page : dirtyPages) {
      page.syncPageBuffer();
      page.flushPageToChannel(channel);
    }
  }

  private void flushSinglePageWithoutLogging(ISchemaPage page) throws IOException {
    page.syncPageBuffer();
    page.flushPageToChannel(channel);
  }

  public synchronized void flushMultiPages(SchemaPageContext cxt) throws IOException {
    flushDirtyPagesStrategy.apply(
        cxt.referredPages.values().stream()
            .filter(ISchemaPage::isDirtyPage)
            .collect(Collectors.toList()));
  }

  public void flushSinglePage(ISchemaPage page) throws IOException {
    singlePageFlushStrategy.apply(page);
  }
  // endregion
}
