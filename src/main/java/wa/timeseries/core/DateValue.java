package wa.timeseries.core;

public class DateValue<T> implements Comparable<DateValue<T>> {
    private final long date;
    private final T value;

    public DateValue(long date, T value) {
        this.date = date;
        this.value = value;
    }

    public long getDate() {
        return date;
    }

    public T getValue() {
        return value;
    }

    @Override public boolean equals(Object obj) {
        if (!this.getClass().equals(obj.getClass())) return false;

        DateValue<T> other = (DateValue<T>) obj;

        return (compareTo(other) == 0);
    }

    @Override public int compareTo(DateValue<T> o) {
        long d = date - o.date;
        if (d != 0) return (int) d;
        return value.hashCode() - o.value.hashCode();
    }
}
