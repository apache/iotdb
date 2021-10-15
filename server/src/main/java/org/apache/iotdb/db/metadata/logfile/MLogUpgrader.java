/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.logfile;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.tag.TagLogFile;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.metadata.logfile.MLogWriter.convertFromString;

public class MLogUpgrader {

  private static final Logger logger = LoggerFactory.getLogger(MLogUpgrader.class);
  private static final String DELETE_FAILED_FORMAT = "Deleting %s failed with exception %s";

  private MLogWriter mLogWriter;
  private MLogTxtReader mLogTxtReader;
  private TagLogFile tagLogFile;

  private String schemaDir;
  private String oldFileName;
  private String newFileName;
  private boolean isSnapshot;

  public MLogUpgrader(
      String schemaDir, String oldFileName, String newFileName, boolean isSnapshot) {
    this.schemaDir = schemaDir;
    this.oldFileName = oldFileName;
    this.newFileName = newFileName;
    this.isSnapshot = isSnapshot;
  }

  public static synchronized void upgradeMLog() throws IOException {
    String schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    MLogUpgrader mLogUpgrader =
        new MLogUpgrader(
            schemaDir, MetadataConstant.METADATA_TXT_LOG, MetadataConstant.METADATA_LOG, false);
    mLogUpgrader.upgradeTxtToBin();
    MLogUpgrader mTreeSnapshotUpgrader =
        new MLogUpgrader(
            schemaDir, MetadataConstant.MTREE_TXT_SNAPSHOT, MetadataConstant.MTREE_SNAPSHOT, true);
    mTreeSnapshotUpgrader.upgradeTxtToBin();
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void upgradeTxtToBin() throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + newFileName);
    File tmpLogFile = SystemFileFactory.INSTANCE.getFile(logFile.getAbsolutePath() + ".tmp");
    File oldLogFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + oldFileName);
    File tmpOldLogFile = SystemFileFactory.INSTANCE.getFile(oldLogFile.getAbsolutePath() + ".tmp");

    if (oldLogFile.exists() || tmpOldLogFile.exists()) {

      if (tmpOldLogFile.exists() && !oldLogFile.exists()) {
        FileUtils.moveFile(tmpOldLogFile, oldLogFile);
      }

      mLogWriter = new MLogWriter(schemaDir, newFileName + ".tmp");
      mLogTxtReader = new MLogTxtReader(schemaDir, oldFileName);
      tagLogFile =
          new TagLogFile(
              IoTDBDescriptor.getInstance().getConfig().getSchemaDir(), MetadataConstant.TAG_LOG);

      // upgrade from old character log file to new binary mlog
      while (mLogTxtReader.hasNext()) {
        String cmd = mLogTxtReader.next();
        if (cmd == null) {
          // no more cmd
          break;
        }
        try {
          operation(cmd, isSnapshot);
        } catch (MetadataException e) {
          logger.error("failed to upgrade cmd {}.", cmd, e);
        }
      }

      // release the .bin.tmp file handler
      mLogWriter.close();
      // rename .bin.tmp to .bin
      FSFactoryProducer.getFSFactory().moveFile(tmpLogFile, logFile);

    } else if (!logFile.exists() && !tmpLogFile.exists()) {
      // if both .bin and .bin.tmp do not exist, nothing to do
    } else if (!logFile.exists() && tmpLogFile.exists()) {
      // if old .bin doesn't exist but .bin.tmp exists, rename tmp file to .bin
      FSFactoryProducer.getFSFactory().moveFile(tmpLogFile, logFile);
    } else if (tmpLogFile.exists()) {
      // if both .bin and .bin.tmp exist, delete .bin.tmp
      try {
        Files.delete(Paths.get(tmpLogFile.toURI()));
      } catch (IOException e) {
        throw new IOException(String.format(DELETE_FAILED_FORMAT, tmpLogFile, e.getMessage()));
      }
    }

    // do some clean job
    // remove old .txt and .txt.tmp
    if (oldLogFile.exists()) {
      try {
        if (mLogTxtReader != null) {
          mLogTxtReader.close();
        }
        Files.delete(Paths.get(oldLogFile.toURI()));
      } catch (IOException e) {
        throw new IOException(String.format(DELETE_FAILED_FORMAT, oldLogFile, e.getMessage()));
      }
    }

    if (tmpOldLogFile.exists()) {
      try {
        Files.delete(Paths.get(tmpOldLogFile.toURI()));
      } catch (IOException e) {
        throw new IOException(String.format(DELETE_FAILED_FORMAT, tmpOldLogFile, e.getMessage()));
      }
    }

    if (mLogWriter != null) {
      mLogWriter.close();
    }
    if (mLogTxtReader != null) {
      mLogTxtReader.close();
    }
    if (tagLogFile != null) {
      tagLogFile.close();
    }
  }

  public synchronized void operation(String cmd, boolean isSnapshot)
      throws IOException, MetadataException {
    if (!isSnapshot) {
      operation(cmd);
    } else {
      PhysicalPlan plan = convertFromString(cmd);
      if (plan != null) {
        mLogWriter.putLog(plan);
      }
    }
  }

  /**
   * upgrade from mlog.txt to mlog.bin
   *
   * @param cmd the old meta operation
   * @throws IOException
   * @throws MetadataException
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public synchronized void operation(String cmd) throws IOException, MetadataException {
    // see createTimeseries() to get the detailed format of the cmd
    String[] args = cmd.trim().split(",", -1);
    switch (args[0]) {
      case MetadataOperationType.CREATE_TIMESERIES:
        if (args.length > 8) {
          String[] tmpArgs = new String[8];
          tmpArgs[0] = args[0];
          int i = 1;
          tmpArgs[1] = "";
          for (; i < args.length - 7; i++) {
            tmpArgs[1] += args[i] + ",";
          }
          tmpArgs[1] += args[i++];
          for (int j = 2; j < 8; j++) {
            tmpArgs[j] = args[i++];
          }
          args = tmpArgs;
        }
        Map<String, String> props = null;
        if (!args[5].isEmpty()) {
          String[] keyValues = args[5].split("&");
          String[] kv;
          props = new HashMap<>();
          for (String keyValue : keyValues) {
            kv = keyValue.split("=");
            props.put(kv[0], kv[1]);
          }
        }

        String alias = null;
        if (!args[6].isEmpty()) {
          alias = args[6];
        }

        long offset = -1L;
        Map<String, String> tags = null;
        Map<String, String> attributes = null;
        if (!args[7].isEmpty()) {
          offset = Long.parseLong(args[7]);
          Pair<Map<String, String>, Map<String, String>> tagAttributePair =
              tagLogFile.read(
                  IoTDBDescriptor.getInstance().getConfig().getTagAttributeTotalSize(), offset);
          tags = tagAttributePair.left;
          attributes = tagAttributePair.right;
        }

        CreateTimeSeriesPlan plan =
            new CreateTimeSeriesPlan(
                new PartialPath(args[1]),
                TSDataType.deserialize((byte) Short.parseShort(args[2])),
                TSEncoding.deserialize((byte) Short.parseShort(args[3])),
                CompressionType.deserialize((byte) Short.parseShort(args[4])),
                props,
                tags,
                attributes,
                alias);

        plan.setTagOffset(offset);
        mLogWriter.createTimeseries(plan);
        break;
      case MetadataOperationType.CREATE_ALIGNED_TIMESERIES:
      case MetadataOperationType.AUTO_CREATE_DEVICE_MNODE:
        logger.warn("Impossible operation!");
        break;
      case MetadataOperationType.DELETE_TIMESERIES:
        if (args.length > 2) {
          StringBuilder tmp = new StringBuilder();
          for (int i = 1; i < args.length - 1; i++) {
            tmp.append(args[i]).append(",");
          }
          tmp.append(args[args.length - 1]);
          args[1] = tmp.toString();
        }
        mLogWriter.deleteTimeseries(
            new DeleteTimeSeriesPlan(Collections.singletonList(new PartialPath(args[1]))));
        break;
      case MetadataOperationType.SET_STORAGE_GROUP:
        try {
          mLogWriter.setStorageGroup(new PartialPath(args[1]));
        }
        // two time series may set one storage group concurrently,
        // that's normal in our concurrency control protocol
        catch (MetadataException e) {
          logger.info("concurrently operate set storage group cmd {} twice", cmd);
        }
        break;
      case MetadataOperationType.DELETE_STORAGE_GROUP:
        mLogWriter.deleteStorageGroup(new PartialPath(args[1]));
        break;
      case MetadataOperationType.SET_TTL:
        mLogWriter.setTTL(new PartialPath(args[1]), Long.parseLong(args[2]));
        break;
      case MetadataOperationType.CHANGE_OFFSET:
        mLogWriter.changeOffset(new PartialPath(args[1]), Long.parseLong(args[2]));
        break;
      case MetadataOperationType.CHANGE_ALIAS:
        mLogWriter.changeAlias(new PartialPath(args[1]), args[2]);
        break;
      default:
        logger.error("Unrecognizable command {}", cmd);
    }
  }
}
