package wa.timeseries.core;

import java.util.Iterator;

public class DateValueIterator<T> implements Iterator<DateValue<T>> {

    private Iterator<SeriesSlice<T>> sliceIterator;

    private long tsStartDate;

    private final long startDate;
    private long endDate;
    private SeriesSlice<T>.SliceIterator currentSliceIterator;
    private OffsetValue<T> nextValue;

    DateValueIterator(Iterator<SeriesSlice<T>> sliceIterator, long tsStartDate,
            long qStartDate, long qEndDate)
    {
        this.sliceIterator = sliceIterator;
        this.tsStartDate = tsStartDate;
        this.startDate = qStartDate - tsStartDate;
        this.endDate = qEndDate - tsStartDate;

        loadNextSlice();

        forwardToStartDate();
    }

    private void forwardToStartDate() {

        do {
            forwardToNextNotNull();
        } while(nextValue != null && nextValue.getOffset() < startDate);

    }

    private void forwardToNextNotNull() {

        nextValue = null;

        if (currentSliceIterator == null) return;

        do {
            if (currentSliceIterator.hasNext()) {
                nextValue = currentSliceIterator.next();
                if (nextValue != null && nextValue.getValue() != null) {
                    break;
                }
            } else {
                loadNextSlice();
                if (currentSliceIterator == null) {
                    nextValue = null;
                    break;
                }
            }
        } while(currentSliceIterator != null);

    }

    private void loadNextSlice()
    {
        if (this.sliceIterator.hasNext()) {
            currentSliceIterator = this.sliceIterator.next().iterator();
        }
        else
        {
            currentSliceIterator = null;
        }
    }

    @Override
    public boolean hasNext() {
        return (nextValue != null && nextValue.getOffset() <= endDate);
    }

    @Override
    public DateValue<T> next() {
        OffsetValue<T> toReturn = nextValue;

        forwardToNextNotNull();

        return new DateValue<>(tsStartDate + toReturn.getOffset(), toReturn.getValue());
    }

    @Override public void remove()
    {
        throw new RuntimeException("Not supported.");
    }
}
