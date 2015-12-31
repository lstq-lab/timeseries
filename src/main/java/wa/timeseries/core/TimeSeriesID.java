package wa.timeseries.core;

/**
 * Unique ID for a TimeSeries.
 * The family should be unique among families
 * The id should be unique within a family.
 */
public class TimeSeriesID implements Comparable<TimeSeriesID> {

    private final String family;
    private final String id;

    public TimeSeriesID(String family, String id) {
        this.family = family;
        this.id = id;
        if (family.contains("-")) {
            throw new RuntimeException("Timeseries family can't contains the character '-'");
        }
    }

    public String getFamily() {
        return family;
    }

    public String getId() {
        return id;
    }

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof  TimeSeriesID)) {
            return false;
        }
        return compareTo((TimeSeriesID) obj) == 0;
    }

    @Override public int hashCode() {
        return family.hashCode() + id.hashCode();
    }

    @Override public int compareTo(TimeSeriesID o) {
        int r = family.compareTo(o.family);
        if (r != 0) return r;
        else return id.compareTo(o.id);
    }

    @Override public String toString() {
        return family + "-" + id;
    }
}
