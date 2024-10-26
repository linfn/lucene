package org.apache.lucene.index;

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;

public class DocValuesWriterProvider {

  public static final DocValuesWriterProvider DEFAULT_PROVIDER = new DocValuesWriterProvider();

  public SegmentDocValuesWriterProvider getSegmentDocValuesWriterProvider(SegmentInfo segmentInfo) {
    return new SegmentDocValuesWriterProvider();
  }

  public static class SegmentDocValuesWriterProvider {
    public NumericDocValuesWriter getNumericDocValuesWriter(FieldInfo fi, Counter bytesUsed) {
      return new NumericDocValuesWriter(fi, bytesUsed);
    }

    public SortedNumericDocValuesWriter getSortedNumericDocValuesWriter(
        FieldInfo fi, Counter bytesUsed) {
      return new SortedNumericDocValuesWriter(fi, bytesUsed);
    }

    public SortedDocValuesWriter getSortedDocValuesWriter(
        FieldInfo fi, Counter bytesUsed, ByteBlockPool docValuesBytePool) {
      return new SortedDocValuesWriter(fi, bytesUsed, docValuesBytePool);
    }

    public SortedSetDocValuesWriter getSortedSetDocValuesWriter(
        FieldInfo fi, Counter bytesUsed, ByteBlockPool docValuesBytePool) {
      return new SortedSetDocValuesWriter(fi, bytesUsed, docValuesBytePool);
    }

    public BinaryDocValuesWriter getBinaryDocValuesWriter(FieldInfo fi, Counter bytesUsed) {
      return new BinaryDocValuesWriter(fi, bytesUsed);
    }

    public void beforeFlush(SegmentWriteState state) {}
  }
}
