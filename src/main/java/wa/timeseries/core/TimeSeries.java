package wa.timeseries.core;


public class TimeSeries<T> {

    private final TimeSeriesID id;

    private DateValue<T> lastValue;

    public TimeSeries(TimeSeriesID id, DateValue<T> lastValue) {
        this.id = id;
        this.lastValue = lastValue;
    }

    public DateValue<T> getLastValue() {
        return lastValue;
    }

    public TimeSeriesID getId() {
        return id;
    }

    public void update(DateValue<T> newerValue) {
        lastValue = newerValue;
    }
}
