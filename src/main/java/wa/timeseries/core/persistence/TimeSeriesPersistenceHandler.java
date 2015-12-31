package wa.timeseries.core.persistence;

import wa.timeseries.core.*;

import java.util.Iterator;

public interface TimeSeriesPersistenceHandler<T>
{
    Iterator<SeriesSlice<T>> fetchSlices(TimeSeriesConfiguration configuration,
            TimeSeriesID tsId, long seqStart, long seqEnd);

    void persist(TimeSeriesConfiguration configuration, TimeSeriesID tsId, SeriesSlice<T> slice);

    Iterator<TimeSeries<T>> getUpdates(TimeSeriesConfiguration configuration, String family,
            long date);

    TimeSeries<T> get(TimeSeriesID tsId);

    void persist(TimeSeries<T> timeSeries);

    SeriesSlice<T> newSlice(long sliceSeq, int sliceSize, int maxResolution);

    TimeSeries<T> createNewTimeSeries(TimeSeriesID tsId);
}
