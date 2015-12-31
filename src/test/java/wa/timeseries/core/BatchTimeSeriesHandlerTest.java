package wa.timeseries.core;

import com.google.common.base.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.persistence.InMemoryTimeSeriesPersistenceHandler;
import wa.timeseries.core.persistence.TimeSeriesPersistenceHandler;

import static org.junit.Assert.assertEquals;

/**
 * Created by wagner on 18/12/15.
 */
public class BatchTimeSeriesHandlerTest {

    private TimeSeriesPersistenceHandler<Long> persistenceHandler = new InMemoryTimeSeriesPersistenceHandler<>();
    private TimeSeriesID TS_ID = new TimeSeriesID("ts","1");

    private BatchTimeSeriesHandler<Long> handler;

    private TimeSeriesHandler<Object> timeSeriesHandler;

    @Before
    public void before() {
        timeSeriesHandler =
                new TimeSeriesHandler<>(2, 1, 0, persistenceHandler);
        handler = new BatchTimeSeriesHandler(timeSeriesHandler, TS_ID);
    }

    @Test
    public void testApplyOperation() throws Exception {

        //Set the values from 0 .. 10
        for(int i = 0; i <= 10; i++) {
            timeSeriesHandler.add(TS_ID, i, new Long(i));
        }

        //apply a +10 in all items from date = 5
        handler.applyOperation(5, 10,
                (date, previousValue) -> {
                    if (!previousValue.isPresent()) {
                        return Optional.absent();
                    }
                    return Optional.of((previousValue.get()).longValue() + 10);
                });

        for(int i = 0; i <= 4; i++) {
            assertEquals(new Long(i), timeSeriesHandler.get(TS_ID, i).get());
        }

        for(int i = 5; i <= 10; i++) {
            assertEquals(new Long(i+10), timeSeriesHandler.get(TS_ID, i).get());
        }
    }
}