package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.io.IOUtils;
import org.apache.thrift.TException;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 */
public class TemplateInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplateInfo.class);

  // StorageGroup read write lock
  private final ReentrantReadWriteLock templateReadWriteLock;

  private final Map<String, Template> templateMap = new ConcurrentHashMap<>();

  private final String snapshotFileName = "template_info.bin";

  public TemplateInfo() throws IOException {
    templateReadWriteLock = new ReentrantReadWriteLock();
  }

  public TGetTemplateResp getMatchedTemplateByName(String name) {
    TGetTemplateResp resp = new TGetTemplateResp();
    try {
      templateReadWriteLock.readLock().lock();
      Template template = templateMap.get(name);
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      resp.setTemplate(Template.template2ByteBuffer(template));
    } catch (Exception e) {
      LOGGER.warn("Error TemplateInfo name", e);
      resp.setStatus(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
    return resp;
  }

  public TGetAllTemplatesResp getAllTemplate() {
    TGetAllTemplatesResp resp = new TGetAllTemplatesResp();
    try {
      templateReadWriteLock.readLock().lock();
      List<ByteBuffer> templates = new ArrayList<>();
      this.templateMap.values().stream()
          .forEach(
              item -> {
                templates.add(Template.template2ByteBuffer(item));
              });
      resp.setTemplateList(templates);
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    } catch (Exception e) {
      LOGGER.warn("Error TemplateInfo name", e);
      resp.setStatus(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
    return resp;
  }

  public TSStatus createTemplate(CreateSchemaTemplatePlan createSchemaTemplatePlan) {
    try {
      templateReadWriteLock.readLock().lock();
      Template template =
          Template.byteBuffer2Template(ByteBuffer.wrap(createSchemaTemplatePlan.getTemplate()));
      Template temp = this.templateMap.get(template.getName());
      if (temp != null && template.getName().equalsIgnoreCase(temp.getName())) {
        LOGGER.error(
            "Failed to create template, because template name {} is exists", template.getName());
        return new TSStatus(TSStatusCode.DUPLICATED_TEMPLATE.getStatusCode());
      }
      this.templateMap.put(template.getName(), template);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      LOGGER.warn("Error to create template", e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    } finally {
      templateReadWriteLock.readLock().unlock();
    }
  }

  private void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(templateMap.size(), outputStream);
    for (Map.Entry<String, Template> entry : templateMap.entrySet()) {
      serializeTemplate(entry.getValue(), outputStream);
    }
  }

  private void serializeTemplate(Template template, OutputStream outputStream) {
    // SerializeUtils.serialize(template.getName(), outputStream);
    try {
      ByteBuffer dataBuffer = template.serialize();
      //      byte[] data = dataBuffer.array();
      //      int length = data.length;
      //      ReadWriteIOUtils.write(length,outputStream);
      ReadWriteIOUtils.write(dataBuffer, outputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void deserialize(InputStream inputStream) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    while (size > 0) {
      Template template = deserializeTemplate(byteBuffer);
      templateMap.put(template.getName(), template);
      size--;
    }
  }

  private Template deserializeTemplate(ByteBuffer byteBuffer) {
    Template template = new Template();
    int length = ReadWriteIOUtils.readInt(byteBuffer);
    byte[] data = ReadWriteIOUtils.readBytes(byteBuffer, length);
    template.deserialize(ByteBuffer.wrap(data));
    return template;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
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

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
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
