package wa.timeseries.core;

import com.google.common.base.Optional;

import java.util.HashMap;

/**
 * This handler coordinate changes in memory and persist a set of changes
 * as one operator, persisting all changes slices. Although there is no
 * transaction.
 *
 * Not thread-safe.
 */
public class BatchTimeSeriesHandler<T> {

    private final TimeSeriesHandler<T> handler;

    private final TimeSeriesID tsId;

    private TimeSeries<T> ts;

    private HashMap<Long, SeriesSlice<T>> updatedSlices = new HashMap<>();

    public BatchTimeSeriesHandler(TimeSeriesHandler<T> handler,
            TimeSeriesID tsId) {
        this.handler = handler;
        this.tsId = tsId;
        ts = handler.get(tsId).or(handler.createNewTimeSeries(tsId));
    }

    public Optional<T> get(long date) {
        final long offset = handler.getAbsoluteOffset(date);
        final long sliceSeq = handler.getSliceIndex(offset);

        return Optional.fromNullable(fetchOrCreateSliceCached(sliceSeq).get(
                handler.getOffsetInsideSlice(offset, sliceSeq)));
    }

    public void set(long date, T value) {
        handler.doAddValue(tsId, date, value, updatedSlices);
        updateNewer(date, value);
    }

    /**
     * Applies a operation on the timeseries, fromDate (inclusive) until the timeseries last value.
     * @param fromDate
     * @param operation
     */
    public void applyOperation(long fromDate, long toDate, UpdateOperation<T> operation) {
        //fetch first slice
        final long startOffset = handler.getAbsoluteOffset(fromDate);
        final long startSliceSeq = handler.getSliceIndex(startOffset);

        final long endOffset = handler.getAbsoluteOffset(toDate);
        final long endSliceSeq = handler.getSliceIndex(endOffset);


        long slice = startSliceSeq;
        int offsetInsideSlice = handler.getOffsetInsideSlice(startOffset, startSliceSeq);

        int endOffsetInsideSlice = handler.getOffsetInsideSlice(endOffset, endSliceSeq);

        DateValue<T> last = null;

        //TODO MOVE THE LOGIC TO THE SLICE CLASS
        while(slice <= endSliceSeq) {

            Optional<SeriesSlice<T>> optionalSlice = fetchSliceCached(slice);

            for (int i = offsetInsideSlice; i < handler.getSliceSize(); ++i) {

                long date = handler.dateOfOffsetSlice(slice, i);
                if (date > toDate){
                    break;
                }

                T oldValue = null;
                if (optionalSlice.isPresent()) {
                    oldValue = optionalSlice.get().getRawData()[i];
                }

                Optional<T> newValue = operation.update(date, Optional.fromNullable(oldValue));

                if (optionalSlice.isPresent()) {
                    final T[] rawValue = optionalSlice.get().getRawData();
                    if (newValue.isPresent()) {
                        rawValue[i] = newValue.get();
                        last = new DateValue<>(date, newValue.get());
                    } else {
                        rawValue[i] =  null;
                    }
                } else {
                    //Create the slice if one don't exist
                    if (newValue.isPresent()) {
                        //use add to initialize the array properly
                        optionalSlice = Optional.of(fetchOrCreateSliceCached(slice));
                        optionalSlice.get().addAtIndex(newValue.get(), i);
                        last = new DateValue<>(date, newValue.get());
                    }
                }
            }

            slice ++;
            offsetInsideSlice = 0;
        }

        if (last != null) {
            updateNewer(last.getDate(), last.getValue());
        }
    }

    public void commit() {
        if (updatedSlices.size() == 0) {
            return;
        }

        for(SeriesSlice<T> slice : updatedSlices.values()) {
            handler.persist(tsId, slice);
        }

        handler.persist(ts);
    }

    private void updateNewer(long date, T value) {
        if (ts.getLastValue() == null || ts.getLastValue().getDate() <= date) {
            ts.update(new DateValue<>(date, value));
        }
    }

    private Optional<SeriesSlice<T>> fetchSliceCached(long sliceSeq) {
        final SeriesSlice<T> cachedSlice = updatedSlices.get(sliceSeq);
        if (cachedSlice != null) {
            return Optional.of(cachedSlice);
        } else {
            final Optional<SeriesSlice<T>> slice = handler.fetchSlice(tsId, sliceSeq);
            if (slice.isPresent()) {
                updatedSlices.put(sliceSeq, slice.get());
            }
            return slice;
        }
    }

    private SeriesSlice<T> fetchOrCreateSliceCached(long sliceSeq) {
        final SeriesSlice<T> cachedSlice = updatedSlices.get(sliceSeq);
        if (cachedSlice != null) {
            return cachedSlice;
        } else {
            final SeriesSlice<T> slice = handler.fetchOrCreateSeriesSlice(tsId, sliceSeq);
            updatedSlices.put(sliceSeq, slice);
            return slice;
        }
    }

}
