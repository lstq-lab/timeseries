package wa.timeseries.core;

public class TimeSeriesConfiguration {
    private final int sliceSize;
    private final int maxResolution;
    private final long startDate;

    public TimeSeriesConfiguration(int sliceSize, int maxResolution, long startDate) {
        this.sliceSize = sliceSize;
        this.maxResolution = maxResolution;
        this.startDate = startDate;
    }

    public int getSliceSize() {
        return sliceSize;
    }

    public int getMaxResolution() {
        return maxResolution;
    }

    public long getStartDate() {
        return startDate;
    }
}
