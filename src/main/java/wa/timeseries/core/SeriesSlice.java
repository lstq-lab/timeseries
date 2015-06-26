package wa.timeseries.core;

import com.google.common.base.Preconditions;

import java.lang.reflect.Array;
import java.util.Iterator;

public class SeriesSlice<T> implements Comparable<SeriesSlice<T>> {
    private long seq;
    private int maxSize;
    private int maxResolution;

    //How to serialize in an list
    private T[] slice;

    SeriesSlice(){}

    public SeriesSlice(long seq, int maxSize, int maxResolution, T[] rawData) {
        this(seq, maxSize, maxResolution);
        Preconditions.checkState(rawData.length == maxSize, "rawData should be the same size as maxSize");
        this.slice = rawData;
    }

    public SeriesSlice(long seq, int maxSize, int maxResolution) {
        this.seq = seq;
        this.maxSize = maxSize;
        this.maxResolution = maxResolution;
    }

    /**
     * @param sliceOffset The offset from the beginning of the Slice
     * @param value The value
     * @return If there were a value for this offset before, return it.
     */
    T add(int sliceOffset, T value) {
        initializeSliceArray(value);

        int i = getIndex(sliceOffset);

        T prevValue = slice[i];

        slice[i] = value;

        return prevValue;
    }

    T get(int sliceOffset)
    {
        if (slice == null) return null;
        int i = getIndex(sliceOffset);
        return slice[i];
    }

    int getIndex(int sliceOffset)
    {
        int index = (int)Math.floor((float)sliceOffset / (float)maxResolution);
        if (index >= maxSize)
        {
            throw new RuntimeException("Overflow detected. This data belongs to another slice");
        }
        return index;
    }

    private synchronized void initializeSliceArray(T object)
    {
        if (slice == null)
        {
            slice = (T[]) Array.newInstance(object.getClass(), maxSize);
        }
    }

    public long getSeq() {
        return seq;
    }

    public int getMaxResolution()
    {
        return maxResolution;
    }

    public SliceIterator iterator()
    {
        return new SliceIterator();
    }

    T[] getRawSliceData()
    {
        return slice;
    }

    public T[] getRawSliceDataCopy()
    {
        return slice.clone();
    }

    @Override
    public int compareTo(SeriesSlice<T> o) {
        return (int)(seq - o.seq);
    }

    @Override
    public int hashCode() {
        return (int)seq;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SeriesSlice)
        {
            SeriesSlice<T> other = (SeriesSlice<T>) obj;
            return other.seq == seq;
        }

        return false;
    }

    public long getSliceOffset0() {
        return seq * maxSize * maxResolution;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public class SliceIterator implements Iterator<OffsetValue<T>> {

        private int index = 0;
        private long currentOffset = getSliceOffset0();

        @Override
        public boolean hasNext() {
            return index < maxSize;
        }

        @Override
        public OffsetValue<T> next() {
            OffsetValue<T>
                value = new OffsetValue<>(currentOffset, slice[index++]);
            currentOffset += maxResolution;
            return value;
        }

        @Override public void remove()
        {
            throw new RuntimeException("Not supported.");
        }
    }

}
