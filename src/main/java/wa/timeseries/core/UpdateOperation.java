package wa.timeseries.core;

import com.google.common.base.Optional;

public interface UpdateOperation<T> {

    Optional<T> update(long date, Optional<T> previousValue);

}
