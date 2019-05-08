/**
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

package read

import (
	//"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
	"tsfile/common/conf"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/compress"
	"tsfile/file/header"
	"tsfile/file/metadata"
)

type TsFileSequenceReader struct {
	fileName      string
	reader        *utils.FileReader
	size          int64
	metadata_pos  int64
	metadata_size int
}

func (f *TsFileSequenceReader) Open(file string) {
	f.fileName = file

	fin, err := os.Open(file)
	if err == nil {
		stat, _ := fin.Stat()
		f.size = stat.Size()

		// get matadata pos&size
		buf := make([]byte, 4)
		_, err := fin.ReadAt(buf, f.size-int64(len(conf.MAGIC_STRING))-4)
		if err == nil {
			f.metadata_size = int(binary.BigEndian.Uint32(buf))
			f.metadata_pos = f.size - int64(len(conf.MAGIC_STRING)) - 4 - int64(f.metadata_size)
		}

		// get pointer ready for reading RowGroupHeader
		f.reader = utils.NewFileReader(fin)
		f.reader.Seek(int64(len(conf.MAGIC_STRING)), io.SeekStart)

	} else {
		log.Println("Failed to open file: " + file)
		panic(err)
	}
}

func (f *TsFileSequenceReader) ReadHeadMagic() string {
	size := len(conf.MAGIC_STRING)
	buf := f.reader.ReadAt(size, 0)

	return string(buf[:])
}

func (f *TsFileSequenceReader) ReadTailMagic() string {
	size := len(conf.MAGIC_STRING)
	buf := f.reader.ReadAt(size, f.size-int64(size))
	return string(buf[:])
}

func (f *TsFileSequenceReader) ReadFileMetadata() *metadata.FileMetaData {
	fileMetadata := new(metadata.FileMetaData)

	data := f.reader.ReadAt(f.metadata_size, f.metadata_pos)
	fileMetadata.Deserialize(data)

	return fileMetadata
}

func (f *TsFileSequenceReader) HasNextRowGroup() bool {
	return f.reader.Pos() < f.metadata_pos
}

func (f *TsFileSequenceReader) ReadRowGroupHeader() *header.RowGroupHeader {
	header := new(header.RowGroupHeader)
	header.Deserialize(f.reader)

	return header
}

func (f *TsFileSequenceReader) ReadChunkHeader() *header.ChunkHeader {
	header := new(header.ChunkHeader)
	header.Deserialize(f.reader)

	return header
}

func (f *TsFileSequenceReader) ReadChunkHeaderAt(offset int64) *header.ChunkHeader {
	f.reader.Seek(offset, io.SeekStart)
	return f.ReadChunkHeader()
}

func (f *TsFileSequenceReader) ReadChunk(header *header.ChunkHeader) []byte {
	return f.reader.ReadSlice(header.GetDataSize())
}

func (f *TsFileSequenceReader) ReadChunkAt(header *header.ChunkHeader, positionOfChunkHeader int64) []byte {
	f.reader.Seek(positionOfChunkHeader, io.SeekStart)
	return f.ReadChunk(header)
}

func (f *TsFileSequenceReader) ReadChunkAndHeader(position int64) []byte {
	header := f.ReadChunkHeaderAt(position)
	length := header.GetSerializedSize() + header.GetDataSize()

	return f.reader.ReadSlice(length)
}

func (f *TsFileSequenceReader) ReadRaw(position int64, length int) []byte {
	f.reader.Seek(position, io.SeekStart)
	return f.reader.ReadSlice(length)
}

func (f *TsFileSequenceReader) ReadPageHeader(dataType constant.TSDataType) *header.PageHeader {
	header := new(header.PageHeader)
	header.Deserialize(f.reader, dataType)

	return header
}

func (f *TsFileSequenceReader) ReadPageHeaderAt(dataType constant.TSDataType, offset int64) *header.PageHeader {
	f.reader.Seek(offset, io.SeekStart)
	return f.ReadPageHeader(dataType)
}

func (f *TsFileSequenceReader) ReadPage(header *header.PageHeader, compression constant.CompressionType) []byte {
	unCompressor := compress.GetDecompressor(compression)
	data := f.reader.ReadSlice(int(header.GetCompressedSize()))

	if unCompressedData, err := unCompressor.Decompress(data); err == nil {
		return unCompressedData
	} else {
		panic(err)
	}
}

func (f *TsFileSequenceReader) Pos() int64 {
	return f.reader.Pos()
}

func (f TsFileSequenceReader) Close() {
	f.reader.Close()
}
