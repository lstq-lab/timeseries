package wa.timeseries.core.persistence;


import wa.timeseries.core.*;

import java.util.*;

public class InMemoryTimeSeriesPersistenceHandler<T> implements TimeSeriesPersistenceHandler<T> {

    private long persistCount;
    private long fetchCount;
    private HashMap<TimeSeriesID, TreeSet<SeriesSlice<T>>> slicesMap = new HashMap<>();
    private HashMap<TimeSeriesID, TimeSeries<T>> tsMap = new HashMap<>();

    @Override
    public Iterator<TimeSeries<T>> getUpdates(TimeSeriesConfiguration configuration, String family,
            long date) {

        List<TimeSeries<T>> result = new ArrayList<>();

        for(Map.Entry<TimeSeriesID, TimeSeries<T>> entry : tsMap.entrySet()) {
            if (entry.getKey().getFamily().equals(family))
            {
                if (entry.getValue().getLastValue() != null &&
                        entry.getValue().getLastValue().getDate() >= date) {
                    result.add(entry.getValue());
                }
            }
        }

        return result.iterator();
    }

    @Override public TimeSeries<T> get(TimeSeriesID tsId) {
        return tsMap.get(tsId);
    }

    @Override public void persist(TimeSeries timeSeries) {
        tsMap.put(timeSeries.getId(), timeSeries);
    }

    @Override
    public Iterator<SeriesSlice<T>> fetchSlices(TimeSeriesConfiguration configuration, TimeSeriesID tsId, long seqStart, long seqEnd) {

        TreeSet<SeriesSlice<T>> slices = getSliceSet(tsId);

        if (slices.isEmpty()) return Collections.emptyIterator();

        final int s = (int) Math.max(0, seqStart);
        final int e = (int) Math.max(0, Math.min(slices.size(), seqEnd));

        final SeriesSlice<T> sFakeKey = new SeriesSlice<>(s, 0, 0);
        final SeriesSlice<T> eFakeKey = new SeriesSlice<>(e, 0, 0);

        fetchCount++;

        return slices.subSet(sFakeKey, true, eFakeKey, true).iterator();
    }

    @Override
    public void persist(TimeSeriesConfiguration configuration,
            TimeSeriesID tsId, SeriesSlice<T> slice) {
        persistCount++;
        getSliceSet(tsId).add(slice);
    }

    private TreeSet<SeriesSlice<T>> getSliceSet(TimeSeriesID tsId)
    {
        TreeSet<SeriesSlice<T>> slices = slicesMap.get(tsId);
        if (slices == null){
            slices = new TreeSet<>();
            slicesMap.put(tsId, slices);
        }

        return slices;
    }

    public Set<SeriesSlice<T>> getSlices(TimeSeriesID tsId)
    {
        return getSliceSet(tsId);
    }

    public long getPersistCount() {
        return persistCount;
    }

    public long getFetchCount() {
        return fetchCount;
    }
}
