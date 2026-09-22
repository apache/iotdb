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

package com.timecho.iotdb.os.io.aws;

import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.conf.provider.AWSS3Config;
import com.timecho.iotdb.os.exception.ObjectStorageException;
import com.timecho.iotdb.os.exception.S3ConnectionException;
import com.timecho.iotdb.os.fileSystem.OSURI;
import com.timecho.iotdb.os.io.IMetaData;
import com.timecho.iotdb.os.io.ObjectStorageConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.stream.Collectors;

public class S3ObjectStorageConnector implements ObjectStorageConnector {

  private static final Logger logger = LoggerFactory.getLogger(S3ObjectStorageConnector.class);
  private static final String RANGE_FORMAT = "bytes=%d-%d";
  private static final String S3_CONNECTION_ERROR =
      "cannot connect to S3 bucket. Please check the endpoint or credential";
  private static final String DEFAULT_ENDPOINT = "yourEndpoint";
  private static final String DEFAULT_REGION = "yourRegion";

  // Bound both a single attempt and the whole read (including retries and response-body reads).
  // Apply this to query reads only; large uploads and copies can legitimately take longer.
  static final AwsRequestOverrideConfiguration READ_REQUEST_CONFIGURATION =
      AwsRequestOverrideConfiguration.builder()
          .apiCallTimeout(Duration.ofMinutes(1))
          .apiCallAttemptTimeout(Duration.ofSeconds(30))
          .build();

  private final AWSS3Config s3config =
      (AWSS3Config) ObjectStorageDescriptor.getInstance().getConfig().getProviderConfig();
  private final S3Client s3Client;
  private final AwsRequestOverrideConfiguration readRequestConfiguration;

  public S3ObjectStorageConnector() {
    readRequestConfiguration = READ_REQUEST_CONFIGURATION;
    S3ClientBuilder builder =
        S3Client.builder()
            .region(Region.of(s3config.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        s3config.getAccessKeyId(), s3config.getAccessKeySecret())));
    if (!DEFAULT_ENDPOINT.equals(s3config.getEndpoint())) {
      builder.endpointOverride(URI.create(s3config.getEndpoint()));
    }
    if (s3config.isEnablePathStyleAccess()) {
      builder.serviceConfiguration(b -> b.pathStyleAccessEnabled(true));
    }
    s3Client = builder.build();
  }

  S3ObjectStorageConnector(
      S3Client s3Client, AwsRequestOverrideConfiguration readRequestConfiguration) {
    this.s3Client = s3Client;
    this.readRequestConfiguration = readRequestConfiguration;
  }

  @Override
  public boolean isConnectorEnabled() {
    return !DEFAULT_REGION.equals(s3config.getRegion());
  }

  @Override
  public boolean doesObjectExist(OSURI osUri) throws ObjectStorageException {
    try {
      HeadObjectRequest req =
          HeadObjectRequest.builder()
              .bucket(osUri.getBucket())
              .key(osUri.getKey())
              .overrideConfiguration(readRequestConfiguration)
              .build();
      s3Client.headObject(req);
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (SdkException e) {
      throw new ObjectStorageException(e);
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
    }
  }

  @Override
  public IMetaData getMetaData(OSURI osUri) throws ObjectStorageException {
    try {
      HeadObjectRequest req =
          HeadObjectRequest.builder()
              .bucket(osUri.getBucket())
              .key(osUri.getKey())
              .overrideConfiguration(readRequestConfiguration)
              .build();
      HeadObjectResponse resp = s3Client.headObject(req);
      return new S3MetaData(resp.contentLength(), resp.lastModified().toEpochMilli());
    } catch (SdkException e) {
      throw new ObjectStorageException(e);
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
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
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
    }
  }

  @Override
  public boolean delete(OSURI osUri) throws ObjectStorageException {
    try {
      DeleteObjectRequest req =
          DeleteObjectRequest.builder().bucket(osUri.getBucket()).key(osUri.getKey()).build();
      s3Client.deleteObject(req);
      return true;
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
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
      s3Client.deleteObject(deleteReq);
      return true;
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
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
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
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
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
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
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
    }
  }

  @Override
  public byte[] getRemoteObject(OSURI osUri, long position, int len) throws ObjectStorageException {
    String rangeStr = String.format(RANGE_FORMAT, position, position + len - 1);
    if (logger.isDebugEnabled()) {
      logger.debug(rangeStr);
    }
    try {
      GetObjectRequest req =
          GetObjectRequest.builder()
              .bucket(osUri.getBucket())
              .key(osUri.getKey())
              .range(rangeStr)
              .overrideConfiguration(readRequestConfiguration)
              .build();
      ResponseBytes<GetObjectResponse> resp = s3Client.getObjectAsBytes(req);
      return resp.asByteArray();
    } catch (Exception | Error e) {
      throw new ObjectStorageException(e);
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
    }
  }

  @Override
  public void copyObject(OSURI srcUri, OSURI destUri) throws ObjectStorageException {
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
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
    }
  }

  @Override
  public void deleteObjectsByPrefix(OSURI prefixUri) throws ObjectStorageException {
    try {
      ListObjectsV2Request listReq =
          ListObjectsV2Request.builder()
              .bucket(prefixUri.getBucket())
              .prefix(prefixUri.getKey())
              .build();
      ListObjectsV2Response listRes;

      do {
        listRes = s3Client.listObjectsV2(listReq);
        if (listRes.contents().size() == 0) {
          break;
        }

        Delete del =
            Delete.builder()
                .objects(
                    listRes.contents().stream()
                        .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                        .collect(Collectors.toList()))
                .build();
        DeleteObjectsRequest deleteObjectsRequest =
            DeleteObjectsRequest.builder().bucket(prefixUri.getBucket()).delete(del).build();
        s3Client.deleteObjects(deleteObjectsRequest);

        listReq =
            ListObjectsV2Request.builder()
                .bucket(prefixUri.getBucket())
                .prefix(prefixUri.getKey())
                .continuationToken(listRes.nextContinuationToken())
                .build();
      } while (listRes.isTruncated());
    } catch (S3Exception e) {
      throw new ObjectStorageException(e);
    } catch (Throwable t) {
      throw new S3ConnectionException(S3_CONNECTION_ERROR);
    }
  }

  @Override
  public void close() {
    s3Client.close();
  }
}
