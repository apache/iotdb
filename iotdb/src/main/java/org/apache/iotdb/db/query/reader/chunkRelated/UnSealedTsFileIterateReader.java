package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.universal.IterateReader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

public class UnSealedTsFileIterateReader extends IterateReader {

  List<IAggregateReader> unSealedResources;

  public UnSealedTsFileIterateReader(TsFileResource unsealedTsFile, Filter filter,
      boolean isReverse)
      throws IOException {
    super(2);
    TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance()
        .get(unsealedTsFile.getFile().getPath(), false);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);

    List<ChunkMetaData> metaDataList = unsealedTsFile.getChunkMetaDatas();
    // reverse chunk metadata list if traversing chunks from behind forward
    if (isReverse && metaDataList != null && !metaDataList.isEmpty()) {
      Collections.reverse(metaDataList);
    }

    // data in unseal tsfile which has been flushed to disk
    FileSeriesReader unSealedReader;
    if (filter == null) {
      unSealedReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      unSealedReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
    }
    unSealedResources = new ArrayList<>();
    // data in flushing memtable
    MemChunkReader memChunkReader = new MemChunkReader(unsealedTsFile.getReadOnlyMemChunk(),
        filter);
    if (isReverse) {
      unSealedResources.add(memChunkReader);
      unSealedResources.add(new FileSeriesReaderAdapter(unSealedReader));
    } else {

      unSealedResources.add(new FileSeriesReaderAdapter(unSealedReader));
      unSealedResources.add(memChunkReader);

    }
  }

  @Override
  public boolean constructNextReader(int idx) {
    currentSeriesReader = unSealedResources.get(idx);
    return true;
  }
}
