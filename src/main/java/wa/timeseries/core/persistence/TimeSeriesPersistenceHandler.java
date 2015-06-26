package wa.timeseries.core.persistence;

import wa.timeseries.core.SeriesSlice;

import java.util.Iterator;

public interface TimeSeriesPersistenceHandler<T>
{
    Iterator<SeriesSlice<T>> fetchSlices(long seqStart, long seqEnd);

    void persist(SeriesSlice<T> slice);
}
