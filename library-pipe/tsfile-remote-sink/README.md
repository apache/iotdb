# TsFile Remote Sink Plugin

## 1. Overview

`tsfile-remote-sink` is an external Pipe Sink plugin for IoTDB, designed for cross-node data synchronization.
Core capabilities include:

- Transferring generated TsFiles and their associated Object files to a remote node via SCP.
- Native support for network bandwidth rate limiting.
- Batch-triggered file emission based on data size and time delay.

## 2. Build Instructions

Run the following command from the repository root to build the complete plugin JAR with dependencies:

```bash
mvn clean package -pl library-pipe/tsfile-remote-sink -am -DskipTests
```

Artifact path:

`library-pipe/tsfile-remote-sink/target/tsfile-remote-sink-<version>-jar-with-dependencies.jar`

## 3. Register Pipe Plugin

Upload the built JAR to a location accessible by IoTDB (for example, all DataNode hosts), then register the plugin:

```sql
CREATE PIPEPLUGIN tsfile_remote_sink
AS 'org.apache.iotdb.pipe.plugin.sink.tsfile.PipeTsFileRemoteSink'
USING URI 'file:///path/to/tsfile-remote-sink-<version>-jar-with-dependencies.jar';
```

## 4. Example Usage

```sql
CREATE PIPE tsfile_remote_pipe
WITH SINK (
  'sink' = 'tsfile_remote_sink',
  'sink.scp.host' = '192.168.1.10',
  'sink.scp.port' = '22',
  'sink.scp.user' = 'root',
  'sink.scp.password' = 'your_password',
  'sink.scp.remote-path' = '/data/iotdb/pipe',
  'sink.scp.object-batch-size-bytes' = '209715200',
  'sink.scp.object-parallelism' = '4',
  'sink.scp.object-waiting-queue-size' = '8',
  'sink.scp.object-thread-keep-alive-seconds' = '60',
  'sink.rate-limit-bytes-per-second' = '10485760' -- Limit to 10 MB/s
);
```

## 5. Configuration Parameters

Note: All `sink.` prefixes in the table below can be equivalently replaced with `connector.` (for example, `connector.scp.host`).

| Parameter                              | Required | Default                                        | Description                                                                                                                       |
|----------------------------------------|----------|------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `scp.host`                             | Yes      | -                                              | Remote host IP or domain for SCP upload.                                                                                          |
| `scp.user`                             | Yes      | -                                              | SSH username for authentication.                                                                                                  |
| `scp.remote-path`                      | Yes      | -                                              | Remote base directory where files are uploaded.                                                                                   |
| `scp.port`                             | No       | `22`                                           | SSH/SCP port.                                                                                                                     |
| `scp.password`                         | No       | Empty                                          | SSH password (can be omitted if key-based authentication is configured).                                                          |
| `scp.object-batch-size-bytes`          | No       | `209715200`                                    | Maximum bytes per SCP object upload batch.                                                                                        |
| `scp.object-parallelism`               | No       | `min(#cores/4, 16)`<br/>(lower-bounded by `1`) | Maximum parallel SCP uploads for object-file batches. Threads are created on demand and reclaimed after idle timeout.             |
| `scp.object-waiting-queue-size`        | No       | Same as `scp.object-parallelism`               | Maximum queued async object-upload tasks. Once the limit is reached, new submissions wait for an existing async upload to finish. |
| `scp.object-thread-keep-alive-seconds` | No       | `60`                                           | Idle timeout in seconds before async SCP object-upload worker threads are reclaimed.                                              |
| `rate-limit-bytes-per-second`          | No       | `-1`                                           | Upload bandwidth limit (bytes/sec). `<= 0` means unlimited.                                                                       |
| `batch.size-bytes`                     | No       | System default                                 | Maximum batch size to trigger file flush/upload.                                                                                  |
| `batch.max-delay-seconds`              | No       | System default                                 | Maximum wait time to trigger file flush/upload.                                                                                   |
