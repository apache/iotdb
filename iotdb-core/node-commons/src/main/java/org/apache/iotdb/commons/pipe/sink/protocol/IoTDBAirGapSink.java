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

package org.apache.iotdb.commons.pipe.sink.protocol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapELanguageConstant;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferSliceReqBuilder;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_TRANSPORT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_TRANSPORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_TRANSPORT_SET;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_TRANSPORT_UDP_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_MAX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_MIN_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_AIR_GAP_E_LANGUAGE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_AIR_GAP_TRANSPORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_AIR_GAP_UDP_PACKET_SIZE_KEY;
import static org.apache.iotdb.commons.utils.BasicStructureSerDeUtil.LONG_LEN;

@TreeModel
@TableModel
public abstract class IoTDBAirGapSink extends IoTDBSink {

  private static final int UDP_ENVELOPE_SIZE = 2 * Integer.BYTES + Long.BYTES;
  private static final int UDP_SLICE_REQUEST_SERIALIZATION_RESERVED_SIZE = 64;

  protected static class AirGapSocket extends Socket {

    private final TEndPoint endPoint;
    private DatagramSocket datagramSocket;
    private InetAddress datagramAddress;
    private boolean isUdp;

    public AirGapSocket(final String ip, final int port) {
      this.endPoint = new TEndPoint(ip, port);
    }

    public TEndPoint getEndPoint() {
      return endPoint;
    }

    public void connectUdp(final int timeoutMs) throws IOException {
      datagramAddress = InetAddress.getByName(endPoint.getIp());
      datagramSocket = new DatagramSocket();
      datagramSocket.connect(datagramAddress, endPoint.getPort());
      datagramSocket.setSoTimeout(timeoutMs);
      isUdp = true;
    }

    public InetAddress getDatagramAddress() {
      return datagramAddress;
    }

    public DatagramSocket getDatagramSocket() {
      return datagramSocket;
    }

    public boolean isUdp() {
      return isUdp;
    }

    @Override
    public boolean isConnected() {
      return isUdp
          ? datagramSocket != null && datagramSocket.isConnected() && !datagramSocket.isClosed()
          : super.isConnected();
    }

    @Override
    public synchronized void setSoTimeout(final int timeout) throws SocketException {
      if (isUdp && datagramSocket != null) {
        datagramSocket.setSoTimeout(timeout);
      } else {
        super.setSoTimeout(timeout);
      }
    }

    @Override
    public synchronized void close() throws IOException {
      if (datagramSocket != null) {
        datagramSocket.close();
      }
      super.close();
    }

    @Override
    public String toString() {
      return "AirGapSocket{"
          + "endPoint="
          + endPoint
          + ", transport="
          + (isUdp ? "udp" : "tcp")
          + "} ("
          + super.toString()
          + ")";
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapSink.class);

  protected static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  protected final List<AirGapSocket> sockets = new ArrayList<>();
  protected final List<Boolean> isSocketAlive = new ArrayList<>();
  private long lastCheckClientStatusTimestamp = 0L;

  private LoadBalancer loadBalancer;
  private long currentClientIndex = 0;

  private int handshakeTimeoutMs;

  private boolean eLanguageEnable;
  private boolean useUdpTransport;
  private int udpPacketSizeInBytes;

  // The air gap connector does not use clientManager thus we put handshake type here
  protected boolean supportModsIfIsDataNodeReceiver = true;

  private final Map<TEndPoint, Long> failLogTimes = new HashMap<>();

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final PipeParameters parameters = validator.getParameters();
    final String airGapTransport =
        parameters
            .getStringOrDefault(
                Arrays.asList(CONNECTOR_AIR_GAP_TRANSPORT_KEY, SINK_AIR_GAP_TRANSPORT_KEY),
                CONNECTOR_AIR_GAP_TRANSPORT_DEFAULT_VALUE)
            .trim()
            .toLowerCase();
    validator.validate(
        arg -> CONNECTOR_AIR_GAP_TRANSPORT_SET.contains(airGapTransport),
        String.format(
            "Air gap transport should be one of %s, but got %s.",
            CONNECTOR_AIR_GAP_TRANSPORT_SET, airGapTransport),
        airGapTransport);

    final int packetSize =
        parameters.getIntOrDefault(
            Arrays.asList(CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_KEY, SINK_AIR_GAP_UDP_PACKET_SIZE_KEY),
            CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_DEFAULT_VALUE);
    validator.validate(
        arg ->
            packetSize >= CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_MIN_VALUE
                && packetSize <= CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_MAX_VALUE,
        String.format(
            "UDP packet size should be in the range [%d, %d], but got %d.",
            CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_MIN_VALUE,
            CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_MAX_VALUE,
            packetSize),
        packetSize);
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    for (int i = 0; i < nodeUrls.size(); i++) {
      isSocketAlive.add(false);
      sockets.add(null);
    }

    switch (loadBalanceStrategy) {
      case CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY:
        loadBalancer = new RoundRobinLoadBalancer();
        break;
      case CONNECTOR_LOAD_BALANCE_RANDOM_STRATEGY:
        loadBalancer = new RandomLoadBalancer();
        break;
      case CONNECTOR_LOAD_BALANCE_PRIORITY_STRATEGY:
        loadBalancer = new PriorityLoadBalancer();
        break;
      default:
        LOGGER.warn(
            "Unknown load balance strategy: {}, use round-robin strategy instead.",
            loadBalanceStrategy);
        loadBalancer = new RoundRobinLoadBalancer();
    }

    handshakeTimeoutMs =
        parameters.getIntOrDefault(
            Arrays.asList(
                CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY, SINK_AIR_GAP_HANDSHAKE_TIMEOUT_MS_KEY),
            CONNECTOR_AIR_GAP_HANDSHAKE_TIMEOUT_MS_DEFAULT_VALUE);
    LOGGER.info(PipeMessages.AIR_GAP_CUSTOMIZED_HANDSHAKE_TIMEOUT, handshakeTimeoutMs);

    eLanguageEnable =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_KEY, SINK_AIR_GAP_E_LANGUAGE_ENABLE_KEY),
            CONNECTOR_AIR_GAP_E_LANGUAGE_ENABLE_DEFAULT_VALUE);
    LOGGER.info(PipeMessages.AIR_GAP_CUSTOMIZED_E_LANGUAGE, eLanguageEnable);

    useUdpTransport =
        CONNECTOR_AIR_GAP_TRANSPORT_UDP_VALUE.equals(
            parameters
                .getStringOrDefault(
                    Arrays.asList(CONNECTOR_AIR_GAP_TRANSPORT_KEY, SINK_AIR_GAP_TRANSPORT_KEY),
                    CONNECTOR_AIR_GAP_TRANSPORT_DEFAULT_VALUE)
                .trim()
                .toLowerCase());
    udpPacketSizeInBytes =
        parameters.getIntOrDefault(
            Arrays.asList(CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_KEY, SINK_AIR_GAP_UDP_PACKET_SIZE_KEY),
            CONNECTOR_AIR_GAP_UDP_PACKET_SIZE_DEFAULT_VALUE);
    LOGGER.info(
        "Air gap transport is {}, udp packet size is {} bytes.",
        useUdpTransport ? "udp" : "tcp",
        udpPacketSizeInBytes);
  }

  @Override
  @SuppressWarnings("java:S2095")
  public void handshake() throws Exception {
    if (System.currentTimeMillis() - lastCheckClientStatusTimestamp
        < PipeConfig.getInstance().getPipeCheckAllSyncClientLiveTimeIntervalMs()) {
      for (int i = 0; i < sockets.size(); i++) {
        if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
          return;
        }
      }
    }

    for (int i = 0; i < sockets.size(); i++) {
      if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
        continue;
      }

      final String ip = nodeUrls.get(i).getIp();
      final int port = nodeUrls.get(i).getPort();

      // Close the socket if necessary
      if (sockets.get(i) != null) {
        try {
          sockets.set(i, null).close();
        } catch (final Exception e) {
          LOGGER.warn(PipeMessages.FAILED_TO_CLOSE_SOCKET, ip, port, e.getMessage());
        }
      }

      final AirGapSocket socket = new AirGapSocket(ip, port);

      try {
        if (useUdpTransport) {
          socket.connectUdp(handshakeTimeoutMs);
        } else {
          socket.connect(new InetSocketAddress(ip, port), handshakeTimeoutMs);
          socket.setKeepAlive(true);
        }
        sockets.set(i, socket);
        LOGGER.info(PipeMessages.CONNECTED_TO_TARGET_SERVER, ip, port);
        failLogTimes.remove(nodeUrls.get(i));
      } catch (final Exception e) {
        final TEndPoint endPoint = nodeUrls.get(i);
        final long currentTimeMillis = System.currentTimeMillis();
        final Long lastFailLogTime = failLogTimes.get(endPoint);
        if (lastFailLogTime == null || currentTimeMillis - lastFailLogTime > 60000) {
          failLogTimes.put(endPoint, currentTimeMillis);
          LOGGER.warn(PipeMessages.FAILED_TO_CONNECT_TO_TARGET, ip, port, e.getMessage());
        }
        continue;
      }

      try {
        sendHandshakeReq(socket);
        isSocketAlive.set(i, true);
      } catch (Exception e) {
        LOGGER.warn(PipeMessages.HANDSHAKE_ERROR_RECEIVING_END, e);
      }
    }

    for (int i = 0; i < sockets.size(); i++) {
      if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
        lastCheckClientStatusTimestamp = System.currentTimeMillis();
        return;
      }
    }
    throw new PipeConnectionException(
        String.format(PipeMessages.ALL_TARGET_SERVERS_NOT_AVAILABLE, nodeUrls));
  }

  protected void sendHandshakeReq(final AirGapSocket socket) throws IOException {
    socket.setSoTimeout(handshakeTimeoutMs);
    // Try to handshake by PipeTransferHandshakeV2Req. If failed, retry to handshake by
    // PipeTransferHandshakeV1Req. If failed again, throw PipeConnectionException.
    if (!send(socket, generateHandShakeV2Payload())) {
      supportModsIfIsDataNodeReceiver = false;
      if (!send(socket, generateHandShakeV1Payload())) {
        throw new PipeConnectionException(PipeMessages.HANDSHAKE_ERROR_WITH_TARGET + socket);
      }
    } else {
      supportModsIfIsDataNodeReceiver = true;
    }
    socket.setSoTimeout(PIPE_CONFIG.getPipeSinkTransferTimeoutMs());
    LOGGER.info(PipeMessages.HANDSHAKE_SUCCESS_SOCKET, socket);
  }

  protected abstract byte[] generateHandShakeV1Payload() throws IOException;

  protected abstract byte[] generateHandShakeV2Payload() throws IOException;

  @Override
  public void heartbeat() {
    try {
      handshake();
    } catch (final Exception e) {
      LOGGER.warn(PipeMessages.FAILED_TO_RECONNECT, e.getMessage(), e);
    }
  }

  protected void transferFilePieces(
      final String pipeName,
      final long creationTime,
      final File file,
      final AirGapSocket socket,
      final boolean isMultiFile)
      throws PipeException, IOException {
    final int readFileBufferSize = PipeConfig.getInstance().getPipeSinkReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
      while (true) {
        mayLimitRateAndRecordIO(readFileBufferSize);
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] payload =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);
        if (!send(
            pipeName,
            creationTime,
            socket,
            isMultiFile
                ? getTransferMultiFilePieceBytes(file.getName(), position, payload)
                : getTransferSingleFilePieceBytes(file.getName(), position, payload))) {
          final String errorMessage =
              String.format("Transfer file %s error. Socket %s.", file, socket);
          if (mayNeedHandshakeWhenFail()) {
            // Send handshake because we don't know whether the receiver side configNode
            // has set up a new one
            sendHandshakeReq(socket);
          }
          receiverStatusHandler.handle(
              new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                  .setMessage(errorMessage),
              errorMessage,
              file.toString());
        } else {
          position += readLength;
        }
      }
    }
  }

  protected abstract void mayLimitRateAndRecordIO(final long requiredBytes);

  protected abstract boolean mayNeedHandshakeWhenFail();

  protected abstract byte[] getTransferSingleFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException;

  protected abstract byte[] getTransferMultiFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException;

  protected int nextSocketIndex() {
    return loadBalancer.nextSocketIndex();
  }

  protected boolean send(
      final String pipeName, final long creationTime, final AirGapSocket socket, byte[] bytes)
      throws IOException {
    bytes = compressIfNeeded(bytes);
    rateLimitIfNeeded(pipeName, creationTime, socket.getEndPoint(), bytes.length);
    return sendBytes(socket, bytes);
  }

  protected boolean sendBytes(final AirGapSocket socket, byte[] bytes) throws IOException {
    if (!socket.isConnected()) {
      throw new SocketException(
          String.format("Socket %s is closed, will try to handshake", socket));
    }

    if (socket.isUdp()) {
      return sendBytesByUdp(socket, bytes);
    }

    final BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
    bytes = enrichWithLengthAndChecksum(bytes);
    outputStream.write(eLanguageEnable ? enrichWithELanguage(bytes) : bytes);
    outputStream.flush();

    final byte[] response = new byte[1];
    final int size = socket.getInputStream().read(response);
    return size > 0 && Arrays.equals(AirGapOneByteResponse.OK, response);
  }

  private boolean sendBytesByUdp(final AirGapSocket socket, final byte[] bytes) throws IOException {
    for (final byte[] requestBytes : sliceIfNeededForUdp(bytes)) {
      if (!sendOneDatagram(socket, requestBytes)) {
        return false;
      }
    }
    return true;
  }

  private boolean sendOneDatagram(final AirGapSocket socket, final byte[] requestBytes)
      throws IOException {
    final byte[] datagramBytes =
        eLanguageEnable
            ? enrichWithELanguage(enrichWithLengthAndChecksum(requestBytes))
            : enrichWithLengthAndChecksum(requestBytes);
    if (datagramBytes.length > udpPacketSizeInBytes) {
      throw new IOException(
          String.format(
              "Air gap UDP datagram size %d exceeds configured packet size %d.",
              datagramBytes.length, udpPacketSizeInBytes));
    }

    final DatagramSocket datagramSocket = socket.getDatagramSocket();
    datagramSocket.send(
        new DatagramPacket(
            datagramBytes,
            datagramBytes.length,
            socket.getDatagramAddress(),
            socket.getEndPoint().getPort()));

    final byte[] response = new byte[1];
    final DatagramPacket responsePacket = new DatagramPacket(response, response.length);
    datagramSocket.receive(responsePacket);
    return responsePacket.getLength() > 0 && Arrays.equals(AirGapOneByteResponse.OK, response);
  }

  private List<byte[]> sliceIfNeededForUdp(final byte[] requestBytes) throws IOException {
    final int rawPayloadSizeLimit =
        udpPacketSizeInBytes
            - UDP_ENVELOPE_SIZE
            - (eLanguageEnable
                ? AirGapELanguageConstant.E_LANGUAGE_PREFIX.length
                    + AirGapELanguageConstant.E_LANGUAGE_SUFFIX.length
                : 0);
    if (requestBytes.length <= rawPayloadSizeLimit) {
      return Arrays.asList(requestBytes);
    }

    final int sliceBodySizeLimit =
        rawPayloadSizeLimit - UDP_SLICE_REQUEST_SERIALIZATION_RESERVED_SIZE;
    if (sliceBodySizeLimit <= 0) {
      throw new IOException(
          String.format(
              "Air gap UDP packet size %d is too small to transfer sliced requests.",
              udpPacketSizeInBytes));
    }

    final TPipeTransferReq request = toTPipeTransferReq(requestBytes);
    final int sliceOrderId = PipeTransferSliceReqBuilder.nextSliceOrderId();
    final int sliceCount = PipeTransferSliceReqBuilder.getSliceCount(request, sliceBodySizeLimit);

    final List<byte[]> slicedRequestBytes = new ArrayList<>(sliceCount);
    for (int i = 0; i < sliceCount; i++) {
      slicedRequestBytes.add(
          toTPipeTransferBytes(
              PipeTransferSliceReqBuilder.buildSliceReq(
                  request, sliceOrderId, i, sliceCount, sliceBodySizeLimit)));
    }
    return slicedRequestBytes;
  }

  private TPipeTransferReq toTPipeTransferReq(final byte[] requestBytes) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(requestBytes);
    final TPipeTransferReq request = new TPipeTransferReq();
    request.version = ReadWriteIOUtils.readByte(byteBuffer);
    request.type = ReadWriteIOUtils.readShort(byteBuffer);
    request.body = byteBuffer.slice();
    return request;
  }

  private byte[] toTPipeTransferBytes(final TPipeTransferReq request) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(request.version, outputStream);
      ReadWriteIOUtils.write(request.type, outputStream);

      final ByteBuffer bodyBuffer = request.body.duplicate();
      final byte[] body = new byte[bodyBuffer.remaining()];
      bodyBuffer.get(body);
      outputStream.write(body);

      return Arrays.copyOf(byteArrayOutputStream.getBuf(), byteArrayOutputStream.size());
    }
  }

  protected boolean send(final AirGapSocket socket, final byte[] bytes) throws IOException {
    return send(null, 0, socket, bytes);
  }

  private byte[] enrichWithLengthAndChecksum(final byte[] bytes) {
    // Length of checksum and bytes payload
    final byte[] length = BytesUtils.intToBytes(bytes.length + LONG_LEN);

    final CRC32 crc32 = new CRC32();
    crc32.update(bytes, 0, bytes.length);

    // Double length as simple checksum
    return BytesUtils.concatByteArrayList(
        Arrays.asList(length, length, BytesUtils.longToBytes(crc32.getValue()), bytes));
  }

  private byte[] enrichWithELanguage(final byte[] bytes) {
    return BytesUtils.concatByteArrayList(
        Arrays.asList(
            AirGapELanguageConstant.E_LANGUAGE_PREFIX,
            bytes,
            AirGapELanguageConstant.E_LANGUAGE_SUFFIX));
  }

  @Override
  public void close() {
    for (int i = 0; i < sockets.size(); ++i) {
      try {
        if (sockets.get(i) != null) {
          sockets.set(i, null).close();
        }
      } catch (final Exception e) {
        LOGGER.warn(PipeMessages.FAILED_TO_CLOSE_CLIENT, i, e);
      } finally {
        isSocketAlive.set(i, false);
      }
    }

    super.close();
  }

  /////////////////////// Strategies for load balance //////////////////////////

  private interface LoadBalancer {
    int nextSocketIndex();
  }

  private class RoundRobinLoadBalancer implements LoadBalancer {
    @Override
    public int nextSocketIndex() {
      final int socketSize = sockets.size();
      // Round-robin, find the next alive client
      for (int tryCount = 0; tryCount < socketSize; ++tryCount) {
        final int clientIndex = (int) (currentClientIndex++ % socketSize);
        if (Boolean.TRUE.equals(isSocketAlive.get(clientIndex))) {
          return clientIndex;
        }
      }

      throw new PipeConnectionException(PipeMessages.ALL_SOCKETS_DEAD);
    }
  }

  private class RandomLoadBalancer implements LoadBalancer {
    @Override
    public int nextSocketIndex() {
      final int socketSize = sockets.size();
      final int clientIndex = (int) (Math.random() * socketSize);
      if (Boolean.TRUE.equals(isSocketAlive.get(clientIndex))) {
        return clientIndex;
      }

      // Random, find the next alive client
      for (int tryCount = 0; tryCount < socketSize - 1; ++tryCount) {
        final int nextClientIndex = (clientIndex + tryCount + 1) % socketSize;
        if (Boolean.TRUE.equals(isSocketAlive.get(nextClientIndex))) {
          return nextClientIndex;
        }
      }

      throw new PipeConnectionException(PipeMessages.ALL_SOCKETS_DEAD);
    }
  }

  private class PriorityLoadBalancer implements LoadBalancer {
    @Override
    public int nextSocketIndex() {
      // Priority, find the first alive client
      final int socketSize = sockets.size();
      for (int i = 0; i < socketSize; ++i) {
        if (Boolean.TRUE.equals(isSocketAlive.get(i))) {
          return i;
        }
      }

      throw new PipeConnectionException(PipeMessages.ALL_SOCKETS_DEAD);
    }
  }
}
