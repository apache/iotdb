package org.apache.iotdb.db.metadata.schemafile;

import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Spliterator;
import java.util.function.Consumer;

public class MTreeBFTIterator implements Iterable<IMNode>, Iterator<IMNode>{

  IMNode rootNode;
  Queue<IMNode> restNodes;

  public MTreeBFTIterator(IMNode rootNode) {
    this.rootNode = rootNode;
    restNodes = new LinkedList<>();
    restNodes.add(rootNode);
  }

  @NotNull
  @Override
  public Iterator<IMNode> iterator() {
    return this;
  }

  @Override
  public void forEach(Consumer<? super IMNode> action) {
    Iterable.super.forEach(action);
  }

  @Override
  public Spliterator<IMNode> spliterator() {
    return Iterable.super.spliterator();
  }

  @Override
  public boolean hasNext() {
    return restNodes.size() > 0;
  }

  @Override
  public IMNode next() {
    IMNode curNode = restNodes.poll();
    for (IMNode child : curNode.getChildren().values()) {
      restNodes.add(child);
    }
    return curNode;
  }

  @Override
  public void remove() {
    Iterator.super.remove();
  }

  @Override
  public void forEachRemaining(Consumer<? super IMNode> action) {
    Iterator.super.forEachRemaining(action);
  }
}
