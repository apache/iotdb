<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Recovery Process

Recovery are performed at the granularity of the storage group, and the entry point for recovery is the recover() of the StorageGroupProcessor

## Recovery Process Of Storage Group

* First get all the data files ending with .tsfile in the storage group, return TsFileResource, there are several file lists as follows

	* Sequence Files
		* 0.10 version tsfiles（sealed/unsealed）
		* 0.9 version tsfiles（sealed）
	* Unsequence Files
		* 0.10 version tsfiles（sealed/unsealed）
		* 0.9 version tsfiles（sealed）


* If there exists 0.9 version TsFile in the storage group, add the old version's sequence and unsequence files to `upgradeSeqFileList` and `upgradeSeqFileList` respectively for upgrade and query.

* Group sequence and unsequence files according to partition -- `Map<Long, List<TsFileResource>>`

* To recover the sequential files of each partition, take the sequential TsFile of each partition obtained in the previous step as a parameter, and call `recoverTsFiles` to recover. This method will put the restored sequence TsFile into the `sequenceFileTreeSet` in the form of TsFileResource. If the TsFile is the last one of this partition and it is not sealed, construct a `TsFileProcessor` object for it and add it to `workSequenceTsFileProcessors`. The specific details of this method will be explained in the next section.

* To recover the disordered files of each partition, take the unsequential TsFile of each partition obtained in the previous step as a parameter, and call `recoverTsFiles` to recover. This method will put the restored unsequence TsFile into the `unSequenceFileList ` in the form of TsFileResource. if the TsFile is the last one in this partition and it is not sealed, a `TsFileProcessor` object must be constructed for it and added to `workUnsequenceTsFileProcessors`. The specific details of this method will be explained in the next section.

* Traverse the `sequenceFileTreeSet` and `unSequenceFileList` obtained in the previous two steps respectively, and update the version number corresponding to the partition

* Check whether there is a Modification file during the merge, and call the `RecoverMergeTask.recoverMerge` method to recover the merge

* Call the `updateLastestFlushedTime()` method to update the `latestTimeForEachDevice`, `partitionLatestFlushedTimeForEachDevice` and `globalLatestFlushedTimeForEachDevice` with sequential tsfile of version 0.9

	* `latestTimeForEachDevice` records the latest timestamp under each partition that all devices have been inserted into (including unflushed and flushed)
	* `partitionLatestFlushedTimeForEachDevice` records the latest timestamp of all devices under each partition that has been flushed. It is used to determine whether a newly inserted point is out of order.
	* `globalLatestFlushedTimeForEachDevice` records the latest timestamp of all devices that have been flushed (a summary of the latest timestamps of each partition)

* Finally traverse `sequenceFileTreeSet`, and use the restored sequence file to update `latestTimeForEachDevice`, `partitionLatestFlushedTimeForEachDevice` and `globalLatestFlushedTimeForEachDevice` again

## Recover a TsFile(Seq/Unseq) of each partiton

* org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor.recoverTsFiles

This method is mainly responsible for traversing all TsFiles passed in and recovering them one by one.

* Construct a `TsFileRecoverPerformer` object to recover the TsFile. The recovery logic is encapsulated in the `recover()` method of `TsFileRecoverPerformer` (details will be explained in the next section), which will return a restored `RestorableTsFileIOWriter `Object.

	* If the recovery process fails, record the log and skip the tsfile

* If the TsFile file is not the last file, or the TsFile file is the last file, but has been closed or marked as closed, just set the `closed` attribute of the `TsFileResource` object corresponding to the TsFile in memory to `true`.

* If the TsFile file can continue to be written, it means that this is the last TsFile of this partition, and it is unsealed, and it will continue to remain unsealed. You need to construct a `TsFileProcessor` object for it and place it in ` workSequenceTsFileProcessors` or `workUnsequenceTsFileProcessors`.

* Finally, put the corresponding `TsFileResource` object in the memory of the restored TsFile into `sequenceFileTreeSet` or `unSequenceFileList`


### Details about recovering a TsFile

* org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer.recover

This method is mainly responsible for the recovery of each specific TsFile file.

* First use the tsfile to construct a `RestorableTsFileIOWriter` object. In the construction method of `RestorableTsFileIOWriter`, the content of the tsfile will be checked and truncated if necessary
	* If there is nothing in this file, write `MAGIC_STRING` and `VERSION_NUMBER` for it, and return directly. At this time, `crashed` is `false`, and `canWrite` is `true`;
	* If there is content in this file, construct a `TsFileSequenceReader` object to parse the content, call the `selfCheck` method, truncate the incomplete content and initialize `truncatedSize` to `HeaderLength`
		* If the content of the file is complete (have a complete header of `MAGIC_STRING` and `VERSION_NUMBER`, and a tail of `MAGIC_STRING`), return `TsFileCheckStatus.COMPLETE_FILE`
		* If the file length is less than `HeaderLength(len(MAGIC_STRING) + len(VERSION_NUMBER))`, or the content of the file header is not `MAGIC_STRING + VERSION_NUMBER `, return `INCOMPATIBLE_FILE`
		* If the file length is exactly equal to `HeaderLength`, and the file content is `MAGIC_STRING + VERSION_NUMBER`, then retunr `HeaderLength`
		* If the file length is greater than `HeaderLength` and the file header is legal, but there is no `MAGIC_STRING` at the end of the file, it means that the file is incomplete and needs to be truncated. Read from `VERSION_NUMBER` position, read out the data in the following chunk, and recover the ChunkMetadata based on the data in the chunk. If you encounter `CHUNK_GROUP_FOOTER`, it means that the entire ChunkGroup is complete. Update `truncatedSize` to the current position
		* Return `truncatedSize`
	* truncated the file according to the returned `truncatedSize`
		* If `truncatedSize` is equal to `TsFileCheckStatus.COMPLETE_FILE`, set `crashed` and `canWrite` to `false`, and close the output stream of the file
		* If `truncatedSize` is equal to `TsFileCheckStatus.INCOMPATIBLE_FILE`, the output stream of the file is closed and an exception is thrown
		* Otherwise, set `crashed` and `canWrite` to `true` and truncated the file to `truncatedSize`

* Judge whether the file is complete by the returned RestorableTsFileIOWriter

	* If the TsFile file is complete
		* If the resource file corresponding to the TsFile exists, the resource file is deserialized (including the minimum and maximum timestamps of each device in the tsfile), and the file version number is restored
		* If the resource file corresponding to the TsFile does not exist, regenerate the resource file and persist it to disk.
		* Return the generated `RestorableTsFileIOWriter`

	* If TsFile is incomplete
		* Call `recoverResourceFromWriter` to recover the resource information through the ChunkMetadata information in `RestorableTsFileIOWriter`
		* Call the `redoLogs` method to write the data in one or more wal files corresponding to this file to a temporary Memtable and persist to this incomplete TsFile
			* For sequential files, skip WALs whose timestamp is less than or equal to the current resource
			* For unsequential files, redo all WAL, it is possible to repeatedly write to ChunkGroup of multiple devices
		* If the TsFile is not the last TsFile of the current partition, or there is a `.closing` file in the TsFile, call the `endFile()` method of `RestorableTsFileIOWriter` to seal the file, delete the `.closing` file and generates resource file for it.