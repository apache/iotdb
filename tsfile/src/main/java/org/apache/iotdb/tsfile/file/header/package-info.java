package org.apache.iotdb.tsfile.file.header;

/**
 ChunkGroupFooter and ChunkHeader are used for parsing file.

 ChunkGroupMetadata and ChunkMetadata are used for locating the positions of ChunkGroup (footer) and chunk (header),
 filtering data quickly, and thereby they have digest information.

 However, because Page has only the header structure, therefore, PageHeader has the both two functions.

 */