package wa.timeseries.core.persistence;


import wa.timeseries.core.SeriesSlice;

import java.util.*;

public class InMemoryTimeSeriesPersistenceHandler<T> implements TimeSeriesPersistenceHandler<T> {

    private TreeSet<SeriesSlice<T>> slices = new TreeSet<>();

    private long persistCount;
    private long fetchCount;

    @Override
    public Iterator<SeriesSlice<T>> fetchSlices(long seqStart, long seqEnd) {

        if (slices.isEmpty()) return Collections.emptyIterator();

        final int s = (int) Math.max(0, seqStart);
        final int e = (int) Math.max(0, Math.min(slices.size(), seqEnd));

        final SeriesSlice<T> sFakeKey = new SeriesSlice<>(s, 0, 0);
        final SeriesSlice<T> eFakeKey = new SeriesSlice<>(e, 0, 0);

        fetchCount++;

        return slices.subSet(sFakeKey, true, eFakeKey, true).iterator();
    }

    @Override
    public void persist(SeriesSlice<T> slice) {
        persistCount++;
        slices.add(slice);
    }

    public Set<SeriesSlice<T>> getSlices()
    {
        return slices;
    }

    public long getPersistCount() {
        return persistCount;
    }

    public long getFetchCount() {
        return fetchCount;
    }
}
