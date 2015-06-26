package wa.timeseries.core;

/**
 * An value in the Time-series.
 * @param <T>
 */
public class OffsetValue<T> implements Comparable<OffsetValue<T>> {

    private long offset;
    private T value;

    OffsetValue(long date, T value) {
        this.offset = date;
        this.value = value;
    }

    public long getOffset() {
        return offset;
    }

    public T getValue() {
        return value;
    }

    @Override public boolean equals(Object obj) {
        if (!this.getClass().equals(obj.getClass())) return false;

        OffsetValue<T> other = (OffsetValue<T>) obj;

        return (compareTo(other) == 0);
    }

    @Override public int compareTo(OffsetValue<T> o) {
        long d = offset - o.offset;
        if (d != 0) return (int) d;
        return value.hashCode() - o.value.hashCode();
    }
}
