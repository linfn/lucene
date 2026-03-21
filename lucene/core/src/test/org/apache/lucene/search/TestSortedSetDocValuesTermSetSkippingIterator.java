/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestSortedSetDocValuesTermSetSkippingIterator extends LuceneTestCase {

  public void testSingleLevel() throws IOException {
    doTestBasics(false);
  }

  public void testMultipleLevels() throws IOException {
    doTestBasics(true);
  }

  public void testIntersectsBoundaries() {
    assertFalse(
        SortedSetDocValuesTermSetSkippingIterator.intersects(new LongBitSet(0), 0, 0));

    LongBitSet termSet = new LongBitSet(11);
    termSet.set(10);
    assertFalse(SortedSetDocValuesTermSetSkippingIterator.intersects(termSet, 5, 4));
    assertFalse(SortedSetDocValuesTermSetSkippingIterator.intersects(termSet, 0, 9));
    assertTrue(SortedSetDocValuesTermSetSkippingIterator.intersects(termSet, 10, 10));
    assertFalse(SortedSetDocValuesTermSetSkippingIterator.intersects(termSet, 11, 12));
  }

  public void testAdvanceReturnsNoMoreDocsWhenNoIntervalsIntersect() throws IOException {
    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    DocIdSetIterator approximation = newApproximation(0, 20);
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(approximation) {
          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            return true;
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };
    DocValuesSkipper skipper =
        newSingleLevelSkipper(
            new int[] {0, 20}, new int[] {9, 29}, new long[] {10, 10}, new long[] {10, 10}, new int[] {1, 1});

    LongBitSet termSet = new LongBitSet(12);
    termSet.set(11);
    SortedSetDocValuesTermSetSkippingIterator setIterator =
        new SortedSetDocValuesTermSetSkippingIterator(twoPhase, skipper, termSet);
    SortedSetDocValuesTermSetSkippingIterator.Approximation setApproximation =
        (SortedSetDocValuesTermSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, setApproximation.advance(0));
    assertFalse(twoPhaseCalled.get());
  }

  public void testAdvanceWhenTargetIsBetweenBlocks() throws IOException {
    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    DocIdSetIterator approximation =
        newApproximation(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(approximation) {
          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            return true;
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };
    DocValuesSkipper skipper =
        newSingleLevelSkipper(
            new int[] {0, 20}, new int[] {9, 29}, new long[] {10, 10}, new long[] {10, 10}, new int[] {10, 10});

    LongBitSet termSet = new LongBitSet(11);
    termSet.set(10);
    SortedSetDocValuesTermSetSkippingIterator setIterator =
        new SortedSetDocValuesTermSetSkippingIterator(twoPhase, skipper, termSet);
    SortedSetDocValuesTermSetSkippingIterator.Approximation setApproximation =
        (SortedSetDocValuesTermSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(20, setApproximation.advance(15));
    assertEquals(SortedSetDocValuesTermSetSkippingIterator.Match.YES, setApproximation.match);
    assertEquals(29, setApproximation.upTo);
    assertTrue(setIterator.matches());
    assertTrue(approximation.docID() < setApproximation.docID());
    assertFalse(twoPhaseCalled.get());
  }

  public void testAdvancePastCurrentBlockWhenApproximationOversteps() throws IOException {
    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    DocIdSetIterator approximation = newApproximation(0, 20);
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(approximation) {
          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            return true;
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };
    DocValuesSkipper skipper =
        newSingleLevelSkipper(
            new int[] {0, 20}, new int[] {9, 29}, new long[] {10, 10}, new long[] {10, 10}, new int[] {1, 1});

    LongBitSet termSet = new LongBitSet(11);
    termSet.set(10);
    SortedSetDocValuesTermSetSkippingIterator setIterator =
        new SortedSetDocValuesTermSetSkippingIterator(twoPhase, skipper, termSet);
    SortedSetDocValuesTermSetSkippingIterator.Approximation setApproximation =
        (SortedSetDocValuesTermSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(20, setApproximation.advance(5));
    assertEquals(
        SortedSetDocValuesTermSetSkippingIterator.Match.IF_DOC_HAS_VALUE,
        setApproximation.match);
    assertEquals(29, setApproximation.upTo);
    assertEquals(approximation.docID(), setApproximation.docID());
    assertTrue(setIterator.matches());
    assertFalse(twoPhaseCalled.get());
  }

  private void doTestBasics(boolean doLevels) throws IOException {
    DocIdSetIterator approximation =
        new DocIdSetIterator() {

          int doc = -1;

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public int nextDoc() throws IOException {
            return advance(doc + 1);
          }

          @Override
          public int advance(int target) throws IOException {
            if (target < 1536) {
              // dense up to 1536
              return doc = target;
            } else if (doc < 2047) {
              // only even docs have a value from 1536 to 2048
              return doc = target + (target & 1);
            } else {
              return doc = NO_MORE_DOCS;
            }
          }

          @Override
          public long cost() {
            return 42;
          }
        };

    AtomicBoolean twoPhaseCalled = new AtomicBoolean();
    TwoPhaseIterator twoPhase =
        new TwoPhaseIterator(approximation) {

          @Override
          public boolean matches() throws IOException {
            twoPhaseCalled.set(true);
            int doc = approximation.docID();
            if (doc < 512) {
              return true;
            } else if (doc < 1024) {
              return false;
            } else if (doc < 1536) {
              return doc % 3 == 2;
            } else {
              return true;
            }
          }

          @Override
          public float matchCost() {
            return 3f;
          }
        };

    DocValuesSkipper skipper =
        new DocValuesSkipper() {

          int doc = -1;

          @Override
          public void advance(int target) throws IOException {
            doc = target;
          }

          @Override
          public int numLevels() {
            return doLevels ? 2 : 1;
          }

          @Override
          public int minDocID(int level) {
            int rangeLog = 8 + level;
            if (doc < 0) {
              return -1;
            } else if (doc >= 2048) {
              return DocIdSetIterator.NO_MORE_DOCS;
            } else {
              int mask = (1 << rangeLog) - 1;
              return doc & ~mask;
            }
          }

          @Override
          public int maxDocID(int level) {
            int rangeLog = 8 + level;
            int minDocID = minDocID(level);
            return switch (minDocID) {
              case -1 -> -1;
              case DocIdSetIterator.NO_MORE_DOCS -> DocIdSetIterator.NO_MORE_DOCS;
              default -> minDocID + (1 << rangeLog) - 1;
            };
          }

          @Override
          public long minValue(int level) {
            int minDocID = minDocID(level);
            if (minDocID < 512) {
              return 10;
            } else if (minDocID < 1024) {
              return 100;
            } else if (minDocID < 1536) {
              return 9;
            } else {
              return 10;
            }
          }

          @Override
          public long maxValue(int level) {
            int minDocID = minDocID(level);
            if (minDocID < 512) {
              return 10;
            } else if (minDocID < 1024) {
              return 100;
            } else if (minDocID < 1536) {
              return 30;
            } else {
              return 10;
            }
          }

          @Override
          public int docCount(int level) {
            int rangeLog = 8 + level;
            if (minDocID(level) < 1536) {
              return 1 << rangeLog;
            } else {
              return 1 << (rangeLog - 1);
            }
          }

          @Override
          public long minValue() {
            return 9;
          }

          @Override
          public long maxValue() {
            return 100;
          }

          @Override
          public int docCount() {
            return 1536 + 256;
          }
        };

    LongBitSet termSet = new LongBitSet(101);
    termSet.set(10);

    SortedSetDocValuesTermSetSkippingIterator setIterator =
        new SortedSetDocValuesTermSetSkippingIterator(twoPhase, skipper, termSet);
    SortedSetDocValuesTermSetSkippingIterator.Approximation setApproximation =
        (SortedSetDocValuesTermSetSkippingIterator.Approximation) setIterator.approximation();

    assertEquals(100, setApproximation.advance(100));
    assertEquals(SortedSetDocValuesTermSetSkippingIterator.Match.YES, setApproximation.match);
    assertEquals(doLevels ? 511 : 255, setApproximation.upTo);
    assertTrue(setIterator.matches());
    assertTrue(approximation.docID() < setApproximation.docID());
    assertFalse(twoPhaseCalled.get());

    assertEquals(1024, setApproximation.advance(600));
    assertEquals(SortedSetDocValuesTermSetSkippingIterator.Match.MAYBE, setApproximation.match);
    assertEquals(1279, setApproximation.upTo);
    for (int i = 0; i < 10; ++i) {
      assertEquals(approximation.docID(), setApproximation.docID());
      assertEquals(twoPhase.matches(), setIterator.matches());
      assertTrue(twoPhaseCalled.get());
      twoPhaseCalled.set(false);
      setApproximation.nextDoc();
    }

    assertEquals(1600, setApproximation.advance(1600));
    assertEquals(
        SortedSetDocValuesTermSetSkippingIterator.Match.IF_DOC_HAS_VALUE,
        setApproximation.match);
    assertEquals(doLevels ? 2047 : 1791, setApproximation.upTo);
    assertEquals(approximation.docID(), setApproximation.docID());
    assertTrue(setIterator.matches());
    assertFalse(twoPhaseCalled.get());

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, setApproximation.advance(2048));
  }

  private DocIdSetIterator newApproximation(int... docsWithValue) {
    return new DocIdSetIterator() {

      int index = -1;
      int doc = -1;

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(doc + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        int nextIndex = index + 1;
        while (nextIndex < docsWithValue.length && docsWithValue[nextIndex] < target) {
          nextIndex++;
        }
        index = nextIndex;
        if (index == docsWithValue.length) {
          return doc = NO_MORE_DOCS;
        }
        return doc = docsWithValue[index];
      }

      @Override
      public long cost() {
        return docsWithValue.length;
      }
    };
  }

  private DocValuesSkipper newSingleLevelSkipper(
      int[] minDocIds, int[] maxDocIds, long[] minValues, long[] maxValues, int[] docCounts) {
    return new DocValuesSkipper() {

      int block = -1;

      @Override
      public void advance(int target) throws IOException {
        int nextBlock = block + 1;
        while (nextBlock < maxDocIds.length && maxDocIds[nextBlock] < target) {
          nextBlock++;
        }
        block = nextBlock;
      }

      @Override
      public int numLevels() {
        return 1;
      }

      @Override
      public int minDocID(int level) {
        if (block < 0) {
          return -1;
        } else if (block >= minDocIds.length) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        return minDocIds[block];
      }

      @Override
      public int maxDocID(int level) {
        if (block < 0) {
          return -1;
        } else if (block >= maxDocIds.length) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        return maxDocIds[block];
      }

      @Override
      public long minValue(int level) {
        return minValues[block];
      }

      @Override
      public long maxValue(int level) {
        return maxValues[block];
      }

      @Override
      public int docCount(int level) {
        return docCounts[block];
      }

      @Override
      public long minValue() {
        return minValues[0];
      }

      @Override
      public long maxValue() {
        return maxValues[maxValues.length - 1];
      }

      @Override
      public int docCount() {
        int count = 0;
        for (int docCount : docCounts) {
          count += docCount;
        }
        return count;
      }
    };
  }
}
