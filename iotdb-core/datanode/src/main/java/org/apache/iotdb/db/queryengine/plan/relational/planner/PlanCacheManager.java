package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;

import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PlanCacheManager {

  private static class SingletonHolder {
    private static final PlanCacheManager INSTANCE = new PlanCacheManager();
  }

  private final Map<String, CachedValue> planCache;

  private PlanCacheManager() {
    planCache = new ConcurrentHashMap<>();
  }

  public static PlanCacheManager getInstance() {
    return SingletonHolder.INSTANCE;
  }

  public void cacheValue(
      String cachedKey,
      PlanNode planNodeTree,
      List<Literal> literalReference,
      DatasetHeader header,
      HashMap<Symbol, Type> symbolMap,
      Map<Symbol, ColumnSchema> assignments,
      List<Expression> expressionList,
      List<String> columnAttributes) {
    planCache.put(
        cachedKey,
        new CachedValue(
            planNodeTree, literalReference, header, symbolMap, assignments, expressionList, columnAttributes));
  }

  public CachedValue getCachedValue(String cacheKey) {
    return planCache.get(cacheKey);
  }

  // TODO add LRU strategy
}
