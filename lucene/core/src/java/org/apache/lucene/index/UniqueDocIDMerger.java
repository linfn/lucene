package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.DocIdSetIterator; // javadocs
import org.apache.lucene.util.PriorityQueue;

public abstract class UniqueDocIDMerger<T extends DocIDMerger.Sub> {

  /** Construct this from the provided subs, specifying the maximum sub count */
  public static <T extends DocIDMerger.Sub> UniqueDocIDMerger<T> of(
      List<T> subs, int maxCount, boolean indexIsSorted) throws IOException {
    if (indexIsSorted && maxCount > 1) {
      return new SortedDocIDMerger<>(subs, maxCount);
    } else {
      return new SequentialDocIDMerger<>(subs);
    }
  }

  /** Construct this from the provided subs */
  public static <T extends DocIDMerger.Sub> UniqueDocIDMerger<T> of(
      List<T> subs, boolean indexIsSorted) throws IOException {
    return of(subs, subs.size(), indexIsSorted);
  }

  /** Reuse API, currently only used by postings during merge */
  public abstract void reset() throws IOException;

  /**
   * Returns null when done. <b>NOTE:</b> after the iterator has exhausted you should not call this
   * method, as it may result in unpredicted behavior.
   */
  public abstract List<T> next() throws IOException;

  private UniqueDocIDMerger() {}

  private static class SequentialDocIDMerger<T extends DocIDMerger.Sub>
      extends UniqueDocIDMerger<T> {

    private final List<T> subs;
    private T current;
    private int nextIndex;

    private SequentialDocIDMerger(List<T> subs) throws IOException {
      this.subs = subs;
      reset();
    }

    @Override
    public void reset() throws IOException {
      if (subs.size() > 0) {
        current = subs.get(0);
        nextIndex = 1;
      } else {
        current = null;
        nextIndex = 0;
      }
    }

    @Override
    public List<T> next() throws IOException {
      while (current.nextMappedDoc() == NO_MORE_DOCS) {
        if (nextIndex == subs.size()) {
          current = null;
          return null;
        }
        current = subs.get(nextIndex);
        nextIndex++;
      }
      return List.of(current);
    }
  }

  private static class SortedDocIDMerger<T extends DocIDMerger.Sub> extends UniqueDocIDMerger<T> {

    private final List<T> subs;
    private final List<T> current;
    private final PriorityQueue<T> queue;
    private int queueMinDocID;

    private SortedDocIDMerger(List<T> subs, int maxCount) throws IOException {
      if (maxCount <= 1) {
        throw new IllegalArgumentException();
      }
      this.subs = subs;
      this.current = new ArrayList<>(subs.size());
      queue =
          new PriorityQueue<T>(maxCount) {
            @Override
            protected boolean lessThan(DocIDMerger.Sub a, DocIDMerger.Sub b) {
              return a.mappedDocID < b.mappedDocID;
            }
          };
      reset();
    }

    private void setQueueMinDocID() {
      if (queue.size() > 0) {
        queueMinDocID = queue.top().mappedDocID;
      } else {
        queueMinDocID = DocIdSetIterator.NO_MORE_DOCS;
      }
    }

    @Override
    public void reset() throws IOException {
      // caller may not have fully consumed the queue:
      queue.clear();
      current.clear();
      for (T sub : subs) {
        if (sub.nextMappedDoc() != NO_MORE_DOCS) {
          queue.add(sub);
        } // else all docs in this sub were deleted; do not add it to the queue!
      }
      setQueueMinDocID();
    }

    @Override
    public List<T> next() throws IOException {
      for (var sub : current) {
        if (sub.nextMappedDoc() != NO_MORE_DOCS) {
          queue.add(sub);
        }
      }
      current.clear();
      setQueueMinDocID();
      if (queueMinDocID == NO_MORE_DOCS) {
        return null;
      }
      while (queue.size() > 0 && queue.top().mappedDocID == queueMinDocID) {
        current.add(queue.pop());
      }
      return current;
    }
  }
}
