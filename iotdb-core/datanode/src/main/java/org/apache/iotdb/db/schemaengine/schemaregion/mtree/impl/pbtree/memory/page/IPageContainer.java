package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.page;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Iterator;
import java.util.function.Predicate;

public interface IPageContainer {

  void put(int regionId, ISchemaPage page);

  void iterateToRemove(Predicate<Pair<Integer, ISchemaPage>> predicate, int targetSize);

  int size();

  Iterator<Pair<Integer, ISchemaPage>> iterator();

  void clear();
}
