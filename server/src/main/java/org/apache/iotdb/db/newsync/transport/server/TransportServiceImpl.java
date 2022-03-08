package org.apache.iotdb.db.newsync.transport.server;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.service.transport.thrift.IdentityInfo;
import org.apache.iotdb.service.transport.thrift.MetaInfo;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;
import org.apache.iotdb.service.transport.thrift.TransportService;
import org.apache.iotdb.service.transport.thrift.TransportStatus;
import org.apache.iotdb.service.transport.thrift.Type;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.apache.iotdb.db.newsync.transport.conf.TransportConfig.getSyncedDir;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.CONFLICT_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.ERROR_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.REBASE_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.RETRY_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.SUCCESS_CODE;
import static org.apache.iotdb.db.sync.conf.SyncConstant.DATA_CHUNK_SIZE;

public class TransportServiceImpl implements TransportService.Iface {
  private static Logger logger = LoggerFactory.getLogger(TransportServiceImpl.class);

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private class CheckResult {
    boolean result;
    String index;

    public CheckResult(boolean result, String index) {
      this.result = result;
      this.index = index;
    }

    public boolean isResult() {
      return result;
    }

    public String getIndex() {
      return index;
    }
  }

  private CheckResult checkStartIndexValid(File file, long startIndex) throws IOException {
    File recordFile = new File(file.getAbsolutePath() + ".record");

    if (!recordFile.exists() && startIndex != 0) {
      logger.error(
          "The start index {} of data sync is not valid. "
              + "The file {} is not exist and start index should equal to 0).",
          startIndex,
          recordFile.getAbsolutePath());
      return new CheckResult(false, "0");
    }

    if (recordFile.exists()) {
      try (InputStream inputStream = new FileInputStream(recordFile);
          BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
        String index = bufferedReader.readLine();

        if ((index == null) || (index.length() == 0)) {
          if (startIndex != 0) {
            logger.error(
                "The start index {} of data sync is not valid. "
                    + "The file {} is not exist and start index is should equal to 0.",
                startIndex,
                recordFile.getAbsolutePath());
            return new CheckResult(false, "0");
          }
        }

        if (Long.parseLong(index) != startIndex) {
          logger.error(
              "The start index {} of data sync is not valid. "
                  + "The start index of the file {} should equal to {}.",
              startIndex,
              recordFile.getAbsolutePath(),
              index);
          return new CheckResult(false, index);
        }
      }
    }

    return new CheckResult(true, "0");
  }

  @Override
  public TransportStatus handshake(IdentityInfo identityInfo) throws TException {
    logger.debug("Invoke handshake method from client ip = {}", identityInfo.address);

    // Version check
    if (!config.getIoTDBMajorVersion(identityInfo.version).equals(config.getIoTDBMajorVersion())) {
      return new TransportStatus(
          ERROR_CODE,
          String.format(
              "Version mismatch: the sender <%s>, the receiver <%s>",
              identityInfo.version, config.getIoTDBVersion()));
    }

    if (!new File(getSyncedDir(identityInfo.getAddress(), identityInfo.getUuid())).exists()) {
      new File(getSyncedDir(identityInfo.getAddress(), identityInfo.getUuid())).mkdirs();
    }
    return new TransportStatus(SUCCESS_CODE, "");
  }

  @Override
  public TransportStatus transportData(
      IdentityInfo identityInfo, MetaInfo metaInfo, ByteBuffer buff, ByteBuffer digest) {
    logger.debug("Invoke transportData method from client ip = {}", identityInfo.address);

    String ipAddress = identityInfo.address;
    String uuid = identityInfo.uuid;
    synchronized (uuid.intern()) {
      Type type = metaInfo.type;
      String fileName = metaInfo.fileName;
      long startIndex = metaInfo.startIndex;

      // Check file start index valid
      if (type == Type.FILE) {
        try {
          CheckResult result =
              checkStartIndexValid(new File(getSyncedDir(ipAddress, uuid), fileName), startIndex);
          if (!result.isResult()) {
            return new TransportStatus(REBASE_CODE, result.getIndex());
          }
        } catch (IOException e) {
          logger.error(e.getMessage());
          return new TransportStatus(ERROR_CODE, e.getMessage());
        }
      }

      // Check buff digest
      int pos = buff.position();
      MessageDigest messageDigest = null;
      try {
        messageDigest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        logger.error(e.getMessage());
        return new TransportStatus(ERROR_CODE, e.getMessage());
      }
      messageDigest.update(buff);
      byte[] digestBytes = new byte[digest.capacity()];
      digest.get(digestBytes);
      if (!Arrays.equals(messageDigest.digest(), digestBytes)) {
        return new TransportStatus(RETRY_CODE, "Data digest check error, retry.");
      }

      if (type != Type.FILE) {

        buff.position(pos);
        int length = buff.capacity();
        byte[] byteArray = new byte[length];
        buff.get(byteArray);
        try (InputStream inputStream = new ByteArrayInputStream(byteArray);
            DataInputStream dataInputStream = new DataInputStream(inputStream)) {
          PipeData pipeData = PipeData.deserialize(dataInputStream);
          // Do with file
          // BufferedPipeDataQueue.offer(pipeData);
        } catch (IOException | IllegalPathException e) {
          e.printStackTrace();
        }
      } else {
        // Write buff to {file}.patch
        buff.position(pos);
        File file = new File(getSyncedDir(ipAddress, uuid), fileName + ".patch");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
          randomAccessFile.seek(startIndex);
          int length = buff.capacity();
          byte[] byteArray = new byte[length];
          buff.get(byteArray);
          randomAccessFile.write(byteArray);
          writeRecordFile(
              new File(getSyncedDir(ipAddress, uuid), fileName + ".record"), startIndex + length);
          logger.debug(
              "Sync "
                  + fileName
                  + " start at "
                  + startIndex
                  + " to "
                  + (startIndex + length)
                  + " is done.");
        } catch (IOException e) {
          logger.error(e.getMessage());
          e.printStackTrace();
          return new TransportStatus(ERROR_CODE, e.getMessage());
        }
      }
    }
    return new TransportStatus(SUCCESS_CODE, "");
  }

  @Override
  public TransportStatus checkFileDigest(
      IdentityInfo identityInfo, MetaInfo metaInfo, ByteBuffer digest) throws TException {
    logger.debug("Invoke checkFileDigest method from client ip = {}", identityInfo.address);

    String ipAddress = identityInfo.getAddress();
    String uuid = identityInfo.getUuid();
    synchronized (uuid.intern()) {
      String fileName = metaInfo.fileName;
      MessageDigest messageDigest = null;
      try {
        messageDigest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        logger.error(e.getMessage());
        return new TransportStatus(ERROR_CODE, e.getMessage());
      }

      try (InputStream inputStream =
          new FileInputStream(new File(getSyncedDir(ipAddress, uuid), fileName + ".patch"))) {
        byte[] block = new byte[DATA_CHUNK_SIZE];
        int length;
        while ((length = inputStream.read(block)) > 0) {
          messageDigest.update(block, 0, length);
        }

        String localDigest = (new BigInteger(1, messageDigest.digest())).toString(16);
        byte[] digestBytes = new byte[digest.capacity()];
        digest.get(digestBytes);
        if (!Arrays.equals(messageDigest.digest(), digestBytes)) {
          logger.error(
              "The file {} digest check error. "
                  + "The local digest is {} (should be equal to {}).",
              fileName,
              localDigest,
              digest);
          new File(getSyncedDir(ipAddress, uuid), fileName + ".record").delete();
          return new TransportStatus(CONFLICT_CODE, "File digest check error.");
        }
      } catch (IOException e) {
        e.printStackTrace();
        return new TransportStatus(ERROR_CODE, e.getMessage());
      }

      return new TransportStatus(SUCCESS_CODE, "");
    }
  }

  @Override
  public SyncResponse heartbeat(IdentityInfo identityInfo, SyncRequest syncRequest)
      throws TException {

    // return ReceiverService.getInstance().recMsg(syncRequest);
    return null;
  }

  private void writeRecordFile(File recordFile, long position) throws IOException {
    File tmpFile = new File(recordFile.getAbsolutePath() + ".tmp");
    FileWriter fileWriter = new FileWriter(tmpFile, false);
    fileWriter.write(String.valueOf(position));
    fileWriter.close();
    Files.move(tmpFile.toPath(), recordFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * release resources or cleanup when a client (a sender) is disconnected (normally or abnormally).
   */
  public void handleClientExit() {
    // TODO: Handle client exit here.
    // do nothing now
  }
}
