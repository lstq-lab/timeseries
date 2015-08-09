package wa.timeseries.core;

import com.google.common.base.Optional;
import wa.timeseries.core.persistence.TimeSeriesPersistenceHandler;

import java.util.*;

public class TimeSeriesHandler<T> {

    private TimeSeriesConfiguration configuration;
    private final TimeSeriesPersistenceHandler persistenceHandler;


    public TimeSeriesHandler(int sliceSize, int maxResolution, long startDate,
            TimeSeriesPersistenceHandler persistenceHandler) {
        this.configuration = new TimeSeriesConfiguration(sliceSize, maxResolution, startDate);
        this.persistenceHandler = persistenceHandler;
    }

    public TimeSeriesHandler(int maxResolution, long startDate,
            TimeSeriesPersistenceHandler persistenceHandler) {
        this.configuration = new TimeSeriesConfiguration(256, maxResolution, startDate);
        this.persistenceHandler = persistenceHandler;
    }

    /**
     * Batch add values to the Time series.
     * @param values
     */
    public void batchAdd(TimeSeriesID tsId, Collection<? extends DateValue<T>> values)
    {
        final HashMap<Long, SeriesSlice<T>> toPersistSlices = new HashMap<>();

        TimeSeries timeSeries = persistenceHandler.get(tsId);

        if (timeSeries == null) {
            timeSeries = new TimeSeries(tsId, null);
        }

        DateValue<T> newer = timeSeries.getLastValue();

        //TODO: Verify if load all slices before hand
        //in just one shot will increase performance.
        for(DateValue<T> value : values)
        {
            if (newer == null || newer.getDate() < value.getDate()) {
                newer = value;
            }

            SeriesSlice<T> slice = doAddValue(tsId, value.getDate(), value.getValue(), toPersistSlices);
            toPersistSlices.put(slice.getSeq(), slice);
            //TODO if the values offset are too sparse, we will hold a lot of data (slices)
            //in the memory. Need to flush some in this case.
        }

        //TODO Add Transaction Or return values that failed to persist.
        for(SeriesSlice<T> slice : toPersistSlices.values())
        {
            persist(tsId, slice);
        }

        if (timeSeries.getLastValue() != newer) {
            timeSeries.update(newer);
            persistenceHandler.persist(timeSeries);
        }
    }

    public void add(TimeSeriesID tsId, long date, T value)
    {
        TimeSeries timeSeries = persistenceHandler.get(tsId);

        if (timeSeries == null) {
            timeSeries = new TimeSeries(tsId, null);
        }

        DateValue<T> newer = timeSeries.getLastValue();


        final SeriesSlice<T> slice = doAddValue(tsId, date, value, Collections
                .<Long, SeriesSlice<T>>emptyMap());
        persist(tsId, slice);

        if (newer == null || newer.getDate() < date) {
            timeSeries.update(new DateValue<>(date, value));
            persistenceHandler.persist(timeSeries);
        }
    }

    /**
     * Returns a list of most up-to-date values for all timeseries within
     * a family. The results will contains only timeseries updated after
     * the given date.
     * @param date
     * @return
     */
    public Iterator<TimeSeries<T>> getUpdates(String family, long date) {
        return persistenceHandler.getUpdates(configuration, family, date);
    }

    public Optional<TimeSeries<T>> get(TimeSeriesID tsid) {
        return Optional.<TimeSeries<T>>fromNullable(persistenceHandler.get(tsid));
    }

    public Optional<T> get(TimeSeriesID tsId, long date)
    {
        final long offset = date - configuration.getStartDate();
        final long sliceSeq = getSliceIndex(offset);

        final Optional<SeriesSlice<T>> sliceOptional = fetchSlice(tsId,
                sliceSeq);

        if (!sliceOptional.isPresent())
        {
            return Optional.absent();
        }

        T value = sliceOptional.get().get(
                getOffsetInsideSlice(offset, sliceSeq));

        return Optional.fromNullable(value);
    }

    public Iterator<DateValue<T>> get(TimeSeriesID tsId, long qStartDate, long qEndDate)
    {
        final long startOffset = qStartDate - configuration.getStartDate();
        final long startSliceSeq = getSliceIndex(startOffset);

        final long endOffset = qEndDate - configuration.getStartDate();
        final long endSliceSeq = getSliceIndex(endOffset);

        Iterator<SeriesSlice<T>> sliceIterator = fetchSlices(tsId,
                startSliceSeq, endSliceSeq);

        return new DateValueIterator(sliceIterator, configuration.getStartDate(), qStartDate, qEndDate);
    }

    private SeriesSlice<T> doAddValue(TimeSeriesID tsId, long date, T value, Map<Long, SeriesSlice<T>> sliceTransactionCache) {
        final long offset = date - configuration.getStartDate();
        final long sliceSeq = getSliceIndex(offset);

        Optional<SeriesSlice<T>> sliceOptional =
                Optional.fromNullable(sliceTransactionCache.get(sliceSeq));

        if (!sliceOptional.isPresent()) {
            sliceOptional = fetchSlice(tsId, sliceSeq);
        }

        final SeriesSlice<T> slice;

        if (!sliceOptional.isPresent())
        {
            slice = new SeriesSlice<>(sliceSeq, configuration.getSliceSize(), configuration.getMaxResolution());
        }
        else
        {
            slice = sliceOptional.get();
        }

        slice.add(getOffsetInsideSlice(offset, sliceSeq), value);

        return slice;
    }

    private int getOffsetInsideSlice(long offset, long sliceSeq)
    {
        long sliceOffset0 = sliceSeq * configuration.getSliceSize() * configuration.getMaxResolution();
        int o = (int)(offset - sliceOffset0);
        if (o < 0) {
            throw new RuntimeException("Something is wrong.");
        }
        return (int)(offset - sliceOffset0);
    }

    private long getSliceIndex(long offset)
    {
        long index = (long)Math.floor((double)offset / (double)(
                configuration.getMaxResolution() * configuration.getSliceSize()));
        return index;
    }

    private Optional<SeriesSlice<T>> fetchSlice(TimeSeriesID tsId, long seq)
    {
        Iterator<SeriesSlice<T>> slices = fetchSlices(tsId, seq, seq);
        if (!slices.hasNext())
        {
            return Optional.absent();
        }

        return Optional.of(slices.next());
    }

    private Iterator<SeriesSlice<T>> fetchSlices(TimeSeriesID tsId, long seqStart, long seqEnd)
    {
        return persistenceHandler.fetchSlices(configuration, tsId, seqStart,
                seqEnd);
    }

    private void persist(TimeSeriesID tsId, SeriesSlice<T> slice)
    {
        persistenceHandler.persist(configuration, tsId, slice);
    }
}
