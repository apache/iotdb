package org.apache.iotdb.db.pipe.event.common.tsfile.parser.table;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.metric.PipeTsfileToTabletMetrics;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.write.record.Tablet;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class TabletInsertionEventIterable implements Iterable<TabletInsertionEvent> {

  private final Iterator<Map.Entry<String, TableSchema>> filteredTableSchemaIterator;
  private final TableQueryExecutor tableQueryExecutor;
  private final long startTime;
  private final long endTime;
  private final PipeTaskMeta pipeTaskMeta;
  private final PipeInsertionEvent sourceEvent;
  private int count;

  public TabletInsertionEventIterable(
      Iterator<Map.Entry<String, TableSchema>> filteredTableSchemaIterator,
      TableQueryExecutor tableQueryExecutor,
      long startTime,
      long endTime,
      PipeTaskMeta pipeTaskMeta,
      PipeInsertionEvent sourceEvent) {
    this.filteredTableSchemaIterator = filteredTableSchemaIterator;
    this.tableQueryExecutor = tableQueryExecutor;
    this.startTime = startTime;
    this.endTime = endTime;
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;
    this.count = 0;
  }

  public int getCount() {
    return count;
  }

  @Override
  public Iterator<TabletInsertionEvent> iterator() {
    return new Iterator<TabletInsertionEvent>() {

      private TsFileInsertionEventTableParserTabletIterator tabletIterator = null;

      @Override
      public boolean hasNext() {
        while (tabletIterator == null || !tabletIterator.hasNext()) {
          if (!filteredTableSchemaIterator.hasNext()) {
            return false;
          }

          final Map.Entry<String, TableSchema> entry = filteredTableSchemaIterator.next();

          try {
            tabletIterator =
                new TsFileInsertionEventTableParserTabletIterator(
                    tableQueryExecutor, entry.getKey(), entry.getValue(), startTime, endTime);
          } catch (final Exception e) {
            throw new PipeException("failed to create TsFileInsertionDataTabletIterator", e);
          }
        }

        return true;
      }

      @Override
      public TabletInsertionEvent next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        final Tablet tablet = tabletIterator.next();
        count++;
        if (sourceEvent != null) {
          PipeTsfileToTabletMetrics.getInstance()
              .markTabletCount(sourceEvent.getPipeName() + '_' + sourceEvent.getCreationTime(), 1);
        }
        final TabletInsertionEvent next;
        if (!hasNext()) {
          next =
              new PipeRawTabletInsertionEvent(
                  Boolean.TRUE,
                  sourceEvent != null ? sourceEvent.getTreeModelDatabaseName() : null,
                  tablet,
                  true,
                  sourceEvent != null ? sourceEvent.getPipeName() : null,
                  sourceEvent != null ? sourceEvent.getCreationTime() : 0,
                  pipeTaskMeta,
                  sourceEvent,
                  true);
        } else {
          next =
              new PipeRawTabletInsertionEvent(
                  Boolean.TRUE,
                  sourceEvent != null ? sourceEvent.getTreeModelDatabaseName() : null,
                  tablet,
                  true,
                  sourceEvent != null ? sourceEvent.getPipeName() : null,
                  sourceEvent != null ? sourceEvent.getCreationTime() : 0,
                  pipeTaskMeta,
                  sourceEvent,
                  false);
        }
        return next;
      }
    };
  }
}
