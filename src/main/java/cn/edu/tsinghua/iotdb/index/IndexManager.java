package cn.edu.tsinghua.iotdb.index;

import cn.edu.tsinghua.iotdb.index.common.IndexManagerException;
import cn.edu.tsinghua.iotdb.index.kvmatch.KvMatchIndex;

import java.util.HashMap;
import java.util.Map;

import static cn.edu.tsinghua.iotdb.index.IndexManager.IndexType.KvIndex;

public class IndexManager {
    private static Map<IndexType, IoTIndex> indexMap = new HashMap<>();

    static{
        indexMap.put(KvIndex, KvMatchIndex.getInstance());
    }

    public static IoTIndex getIndexInstance(IndexType indexType){
        return indexMap.get(indexType);
    }


    public enum IndexType {
        KvIndex;
        public static IndexType getIndexType(String indexNameString) throws IndexManagerException {
            String normalized = indexNameString.toLowerCase();
            switch (normalized){
                case "kvindex":
                case "kv-match":
                    return KvIndex;
                default:
                    throw new IndexManagerException("unsupport index type:" + indexNameString);
            }
        }
    }
}
