package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;

public class PriorityMergeReader implements IPointReader {

  private List<IPointReader> readerList = new ArrayList<>();
  private List<Integer> priorityList = new ArrayList<>();
  private PriorityQueue<Element> heap = new PriorityQueue<>();

  public void addReaderWithPriority(IPointReader reader, int priority) throws IOException {
    if (reader.hasNext()) {
      heap.add(new Element(readerList.size(), reader.next(), priority));
    }
    readerList.add(reader);
    priorityList.add(priority);
  }

  @Override
  public boolean hasNext() {
    return !heap.isEmpty();
  }

  @Override
  public TimeValuePair next() throws IOException {
    Element top = heap.peek();
    updateHeap(top);
    return top.timeValuePair;
  }

  @Override
  public TimeValuePair current() {
    return heap.peek().timeValuePair;
  }

  private void updateHeap(Element top) throws IOException {
    while (!heap.isEmpty() && heap.peek().timeValuePair.getTimestamp() == top.timeValuePair
        .getTimestamp()) {
      Element e = heap.poll();
      IPointReader reader = readerList.get(e.index);
      if (reader.hasNext()) {
        heap.add(new Element(e.index, reader.next(), priorityList.get(e.index)));
      }
    }
  }

  @Override
  public void close() throws IOException {
    for (IPointReader reader : readerList) {
      reader.close();
    }
  }

  protected class Element implements Comparable<Element> {

    int index;
    TimeValuePair timeValuePair;
    Integer priority;

    public Element(int index, TimeValuePair timeValuePair, int priority) {
      this.index = index;
      this.timeValuePair = timeValuePair;
      this.priority = priority;
    }

    @Override
    public int compareTo(
        Element o) {

      if (this.timeValuePair.getTimestamp() > o.timeValuePair.getTimestamp()) {
        return 1;
      }

      if (this.timeValuePair.getTimestamp() < o.timeValuePair.getTimestamp()) {
        return -1;
      }

      return o.priority.compareTo(this.priority);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Element) {
        Element element = (Element) o;
        if (this.timeValuePair.getTimestamp() == element.timeValuePair.getTimestamp()
            && this.priority.equals(element.priority)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int) (timeValuePair.getTimestamp() * 31 + priority.hashCode());
    }
  }
}
