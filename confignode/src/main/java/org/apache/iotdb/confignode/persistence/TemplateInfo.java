package org.apache.iotdb.confignode.persistence;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.SerializeUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 **/
public class TemplateInfo implements SnapshotProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateInfo.class);

    // StorageGroup read write lock
    private final ReentrantReadWriteLock templateReadWriteLock;

    private Map<String, Template> templateMap = new ConcurrentHashMap<>();

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
            resp.setTemplate(template2ByteBuffer(template));
        }catch (Exception e) {
            LOGGER.warn("Error TemplateInfo name", e);
            resp.setStatus(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
        }finally {
            templateReadWriteLock.readLock().unlock();
        }
        return resp;
    }

    public TGetAllTemplatesResp getAllTemplate() {
        TGetAllTemplatesResp resp = new TGetAllTemplatesResp();
        try {
            templateReadWriteLock.readLock().lock();
            List<ByteBuffer> templates = new ArrayList<>();
            this.templateMap.values().stream().forEach(item->{
                templates.add(template2ByteBuffer(item));
            });
            resp.setTemplateList(templates);
            resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
        }catch (Exception e) {
            LOGGER.warn("Error TemplateInfo name", e);
            resp.setStatus(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
        }finally {
            templateReadWriteLock.readLock().unlock();
        }
        return resp;
    }

    public ByteBuffer template2ByteBuffer(Template template) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        ReadWriteIOUtils.writeObject(template,dataOutputStream);
        return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    }

    public Template byteBuffer2Template(ByteBuffer byteBuffer) throws IOException,ClassNotFoundException{
        ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(byteBuffer.array());
        ObjectInputStream ois = new ObjectInputStream(byteArrayOutputStream);
        Template template = (Template)ois.readObject();
        return template;
    }

    public TSStatus createTemplate(Template template) {
        try {
            templateReadWriteLock.readLock().lock();
            Template temp = this.templateMap.get(template.getName());
            if(template.getName().equalsIgnoreCase(temp.getName())) {
                LOGGER.error("Failed to create template, because template name {} is exists",template.getName());
                return new TSStatus(TSStatusCode.DUPLICATED_TEMPLATE.getStatusCode());
            }
            this.templateMap.put(template.getName(),template);
            return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
        }catch (Exception e){
            LOGGER.warn("Error to create template", e);
            return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
        }finally {
            templateReadWriteLock.readLock().unlock();
        }
    }

    private void serialize(DataOutputStream outputStream) throws IOException {
        ReadWriteIOUtils.write(templateMap.size(), outputStream);
        for (Map.Entry<String, Template> entry : templateMap.entrySet()) {
            serializeTemplate(entry.getValue(),outputStream);
        }
    }

    private void serializeTemplate(Template template,DataOutputStream outputStream){
        //SerializeUtils.serialize(template.getName(), outputStream);
        try {
            ReadWriteIOUtils.write(template.getName(), outputStream);
            outputStream.write(template.getSchemaMap().size());
            for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
                SerializeUtils.serialize(entry.getKey(), outputStream);
                entry.getValue().partialSerializeTo(outputStream);
            }
        }catch (IOException e) {

        }
    }

    private void deserialize(InputStream inputStream) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(ReadWriteIOUtils.readBytes(inputStream,inputStream.available()));
        int size = ReadWriteIOUtils.readInt(inputStream);
        while (size > 0) {
            Template template = deserializeTemplate(byteBuffer);
            templateMap.put(template.getName(),template);
            size--;
        }
    }

    private Template deserializeTemplate(ByteBuffer byteBuffer){
        Template template = new Template();
        template.deserialize(byteBuffer);
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
            DataOutputStream outputStream = new DataOutputStream(fileOutputStream)) {
            serialize(outputStream);
            outputStream.flush();
            return tmpFile.renameTo(snapshotFile);
        }finally {
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
            deserialize(fileInputStream);
        } finally {
            templateReadWriteLock.writeLock().unlock();
        }
    }
}
