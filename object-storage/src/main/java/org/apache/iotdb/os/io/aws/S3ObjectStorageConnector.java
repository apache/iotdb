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

package org.apache.iotdb.os.io.aws;

import org.apache.iotdb.os.exception.ObjectStorageException;
import org.apache.iotdb.os.fileSystem.OSURI;
import org.apache.iotdb.os.io.IMetaData;
import org.apache.iotdb.os.io.ObjectStorageConnector;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.InputStream;

public class S3ObjectStorageConnector implements ObjectStorageConnector {
  private static final String RANGE_FORMAT = "%d-%d";
  private static final S3Client s3Client =
      S3Client.builder()
          .region(Region.of(AWSS3Config.getRegion()))
          .credentialsProvider(AWSS3Config.getCredentialProvider())
          .build();

  @Override
  public boolean doesObjectExist(OSURI osUri) throws ObjectStorageException {
    try {
      HeadObjectRequest req =
          HeadObjectRequest.builder().bucket(osUri.getBucket()).key(osUri.getKey()).build();
      s3Client.headObject(req);
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public IMetaData getMetaData(OSURI osUri) throws ObjectStorageException {
    try {
      HeadObjectRequest req =
          HeadObjectRequest.builder().bucket(osUri.getBucket()).key(osUri.getKey()).build();
      HeadObjectResponse resp = s3Client.headObject(req);
      return new S3MetaData(resp.contentLength(), resp.lastModified().toEpochMilli());
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public boolean createNewEmptyObject(OSURI osUri) throws ObjectStorageException {
    try {
      PutObjectRequest req =
          PutObjectRequest.builder().bucket(osUri.getBucket()).key(osUri.getKey()).build();
      s3Client.putObject(req, RequestBody.empty());
      return true;
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public boolean delete(OSURI osUri) throws ObjectStorageException {
    try {
      DeleteObjectRequest req =
          DeleteObjectRequest.builder().bucket(osUri.getBucket()).key(osUri.getKey()).build();
      DeleteObjectResponse resp = s3Client.deleteObject(req);
      return resp.deleteMarker();
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public boolean renameTo(OSURI fromOSUri, OSURI toOSUri) throws ObjectStorageException {
    try {
      CopyObjectRequest copyReq =
          CopyObjectRequest.builder()
              .sourceBucket(fromOSUri.getBucket())
              .sourceKey(fromOSUri.getKey())
              .destinationBucket(toOSUri.getBucket())
              .destinationKey(toOSUri.getKey())
              .build();
      s3Client.copyObject(copyReq);

      DeleteObjectRequest deleteReq =
          DeleteObjectRequest.builder()
              .bucket(fromOSUri.getBucket())
              .key(fromOSUri.getKey())
              .build();
      DeleteObjectResponse resp = s3Client.deleteObject(deleteReq);
      return resp.deleteMarker();
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  public OSURI[] list(OSURI osUri) throws ObjectStorageException {
    try {
      ListObjectsRequest req =
          ListObjectsRequest.builder().bucket(osUri.getBucket()).prefix(osUri.getKey()).build();
      return s3Client.listObjects(req).contents().stream()
          .map(obj -> new OSURI(osUri.getBucket(), obj.key()))
          .toArray(OSURI[]::new);
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public InputStream getInputStream(OSURI osUri) throws ObjectStorageException {
    try {
      GetObjectRequest req =
          GetObjectRequest.builder().bucket(osUri.getBucket()).key(osUri.getKey()).build();
      return s3Client.getObject(req);
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public void putLocalFile(OSURI osUri, File lcoalFile) throws ObjectStorageException {
    try {
      PutObjectRequest req =
          PutObjectRequest.builder().bucket(osUri.getBucket()).key(osUri.getKey()).build();
      s3Client.putObject(req, RequestBody.fromFile(lcoalFile));
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public byte[] getRemoteFile(OSURI osUri, long position, int len) throws ObjectStorageException {
    String rangeStr = String.format(RANGE_FORMAT, position, position + len - 1);
    try {
      GetObjectRequest req =
          GetObjectRequest.builder()
              .bucket(osUri.getBucket())
              .key(osUri.getKey())
              .range(rangeStr)
              .build();
      ResponseBytes<GetObjectResponse> resp = s3Client.getObjectAsBytes(req);
      return resp.asByteArray();
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  @Override
  public void copyRemoteFile(OSURI srcUri, OSURI destUri) throws ObjectStorageException {
    try {
      CopyObjectRequest req =
          CopyObjectRequest.builder()
              .sourceBucket(srcUri.getBucket())
              .sourceKey(srcUri.getKey())
              .destinationBucket(destUri.getBucket())
              .destinationKey(destUri.getKey())
              .build();
      s3Client.copyObject(req);
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    }
  }

  public void close() {
    s3Client.close();
  }
}
