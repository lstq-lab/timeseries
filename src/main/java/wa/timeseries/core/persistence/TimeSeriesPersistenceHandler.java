package wa.timeseries.core.persistence;

import wa.timeseries.core.SeriesSlice;
import wa.timeseries.core.TimeSeries;
import wa.timeseries.core.TimeSeriesConfiguration;
import wa.timeseries.core.TimeSeriesID;

import java.util.Iterator;
import java.util.List;

public interface TimeSeriesPersistenceHandler<T>
{
    Iterator<SeriesSlice<T>> fetchSlices(TimeSeriesConfiguration configuration,
            TimeSeriesID tsId, long seqStart, long seqEnd);

    void persist(TimeSeriesConfiguration configuration, TimeSeriesID tsId, SeriesSlice<T> slice);

    Iterator<TimeSeries<T>> getUpdates(TimeSeriesConfiguration configuration, String family,
            long date);

    TimeSeries<T> get(TimeSeriesID tsId);

    void persist(TimeSeries timeSeries);
}
