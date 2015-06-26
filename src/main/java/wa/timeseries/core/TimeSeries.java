package wa.timeseries.core;

import com.google.common.base.Optional;
import wa.timeseries.core.persistence.TimeSeriesPersistenceHandler;

import java.util.*;

public class TimeSeries<T> {
    private final int sliceSize;
    private final int maxResolution;
    private final long startDate;
    private final TimeSeriesPersistenceHandler persistenceHandler;

    public TimeSeries(int sliceSize, int maxResolution, long startDate, TimeSeriesPersistenceHandler persistenceHandler) {
        this.sliceSize = sliceSize;
        this.maxResolution = maxResolution;
        this.startDate = startDate;
        this.persistenceHandler = persistenceHandler;
    }

    public TimeSeries(int maxResolution, long startDate, TimeSeriesPersistenceHandler persistenceHandler) {
        this.sliceSize = 256;
        this.maxResolution = maxResolution;
        this.startDate = startDate;
        this.persistenceHandler = persistenceHandler;
    }

    /**
     * Batch add values to the Time series.
     * @param values
     */
    public void batchAdd(Collection<? extends DateValue<T>> values)
    {
        final HashMap<Long, SeriesSlice<T>> toPersisteSlices = new HashMap<>();

        //TODO: Verify if load all slices before hand
        //in just one shot will increase performance.
        for(DateValue<T> value : values)
        {
            SeriesSlice<T> slice = doAddValue(value.getDate(), value.getValue(), toPersisteSlices);
            toPersisteSlices.put(slice.getSeq(), slice);
            //TODO if the values offset are too sparse, we will hold a lot of data (slices)
            //in the memory. Need to flush some in this case.
        }

        //TODO Add Transaction Or return values that failed to persist.
        for(SeriesSlice<T> slice : toPersisteSlices.values())
        {
            persist(slice);
        }
    }

    public void add(long date, T value)
    {
        final SeriesSlice<T> slice = doAddValue(date, value, Collections.<Long, SeriesSlice<T>>emptyMap());
        persist(slice);
    }

    private SeriesSlice<T> doAddValue(long date, T value, Map<Long, SeriesSlice<T>> sliceTransactionCache) {
        final long offset = date - startDate;
        final long sliceSeq = getSliceIndex(offset);

        Optional<SeriesSlice<T>> sliceOptional =
                Optional.fromNullable(sliceTransactionCache.get(sliceSeq));

        if (!sliceOptional.isPresent()) {
            sliceOptional = fetchSlice(sliceSeq);
        }

        final SeriesSlice<T> slice;

        if (!sliceOptional.isPresent())
        {
            slice = new SeriesSlice<>(sliceSeq, sliceSize, maxResolution);
        }
        else
        {
            slice = sliceOptional.get();
        }

        slice.add(getOffsetInsideSlice(offset, sliceSeq), value);

        return slice;
    }

    public Optional<T> get(long date)
    {
        final long offset = date - startDate;
        final long sliceSeq = getSliceIndex(offset);

        final Optional<SeriesSlice<T>> sliceOptional = fetchSlice(sliceSeq);

        if (!sliceOptional.isPresent())
        {
            return Optional.absent();
        }

        T value = sliceOptional.get().get(getOffsetInsideSlice(offset, sliceSeq));

        return Optional.fromNullable(value);
    }

    public Iterator<DateValue<T>> get(long qStartDate, long qEndDate)
    {
        final long startOffset = qStartDate - startDate;
        final long startSliceSeq = getSliceIndex(startOffset);

        final long endOffset = qEndDate - startDate;
        final long endSliceSeq = getSliceIndex(endOffset);

        Iterator<SeriesSlice<T>> sliceIterator = fetchSlices(startSliceSeq, endSliceSeq);

        return new DateValueIterator(sliceIterator, startDate, qStartDate, qEndDate);
    }

    private int getOffsetInsideSlice(long offset, long sliceSeq)
    {
        long sliceOffset0 = sliceSeq * sliceSize * maxResolution;
        return (int)(offset - sliceOffset0);
    }

    private long getSliceIndex(long sliceOffset)
    {
        long index = (long)Math.floor((float)sliceOffset / (float)(maxResolution*sliceSize));
        return index;
    }

    private Optional<SeriesSlice<T>> fetchSlice(long seq)
    {
        Iterator<SeriesSlice<T>> slices = fetchSlices(seq, seq);
        if (!slices.hasNext())
        {
            return Optional.absent();
        }

        return Optional.of(slices.next());
    }

    private Iterator<SeriesSlice<T>> fetchSlices(long seqStart, long seqEnd)
    {
        return persistenceHandler.fetchSlices(seqStart, seqEnd);
    }


    private void persist(SeriesSlice<T> slice)
    {
        persistenceHandler.persist(slice);
    }

}
