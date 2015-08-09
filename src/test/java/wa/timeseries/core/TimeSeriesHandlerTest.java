package wa.timeseries.core;

import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.persistence.InMemoryTimeSeriesPersistenceHandler;

import java.util.*;

import static org.junit.Assert.*;

public class TimeSeriesHandlerTest {

    private InMemoryTimeSeriesPersistenceHandler<Integer> persistenceHandler;

    private TimeSeriesID TS_ID = new TimeSeriesID("ts","1");


    @Before
    public void before()
    {
        persistenceHandler = new InMemoryTimeSeriesPersistenceHandler<>();
    }

    @Test
    public void testGetUpdates() {
        TimeSeriesHandler<Integer>
                ts = new TimeSeriesHandler<>(1, 0, persistenceHandler);

        ts.add(TS_ID, 10, 0);
        ts.add(TS_ID, 11, 1);

        TimeSeriesID TS_ID_2 = new TimeSeriesID("ts","2");
        ts.add(TS_ID_2, 9, 0);

        Iterator<TimeSeries<Integer>> updates =
                ts.getUpdates(TS_ID_2.getFamily(), 10);
        assertTrue(updates.hasNext());
        TimeSeries<Integer> update = updates.next();
        assertFalse(updates.hasNext());

        assertEquals(TS_ID, update.getId());
        assertEquals(1, update.getLastValue().getValue().intValue());
        assertEquals(11, update.getLastValue().getDate());
    }

    @Test
    public void testAdd() {

        TimeSeriesHandler<Integer>
                ts = new TimeSeriesHandler<>(1, 0, persistenceHandler);
        ts.add(TS_ID, 0, 0);
        assertEquals(0, (int)ts.get(TS_ID, 0).get());
        ts.add(TS_ID, 1, 1);
        assertEquals(1, (int)ts.get(TS_ID, 1).get());

        assertEquals(1, persistenceHandler.getSlices(TS_ID).size());
    }

    @Test
    public void testAddSecondSlice() {
        TimeSeriesHandler<Integer>
                ts = new TimeSeriesHandler<>(1, 0, persistenceHandler);

        ts.add(TS_ID, 0, 0);
        assertEquals(0, (int) ts.get(TS_ID, 0).get());
        ts.add(TS_ID, 256, 256);
        assertEquals(256, (int) ts.get(TS_ID, 256).get());

        assertEquals(2, persistenceHandler.getSlices(TS_ID).size());

        ts.add(TS_ID, 257, 257);
        assertEquals(257, (int) ts.get(TS_ID, 257).get());
        ts.add(TS_ID, 258, 258);
        assertEquals(258, (int) ts.get(TS_ID, 258).get());

        assertEquals(2, persistenceHandler.getSlices(TS_ID).size());

        ts.add(TS_ID, 512, 512);
        assertEquals(512, (int) ts.get(TS_ID, 512).get());
        assertEquals(3, persistenceHandler.getSlices(TS_ID).size());
    }

    @Test
    public void testDataValueIterator()
    {
        Collection<DateValue<Integer>> batch = createBatch(10000, 5, 0);
        TimeSeriesHandler<Integer>
                ts = new TimeSeriesHandler<>(10, 5, 0, persistenceHandler);
        ts.batchAdd(TS_ID, batch);

        Map<Long, DateValue<Integer>> values = toMap(ts.get(TS_ID, 0, 256));

        assertEquals(0, values.get(0l).getValue().intValue());
        assertEquals(255, values.get(255l).getValue().intValue());
    }

    @Test
    public void testDataValueIterator1()
    {
        Collection<DateValue<Integer>> batch = createBatch(10000, 1, 0);
        TimeSeriesHandler<Integer>
                ts = new TimeSeriesHandler<>(10, 1, 0, persistenceHandler);
        ts.batchAdd(TS_ID, batch);

        Map<Long, DateValue<Integer>> values = toMap(ts.get(TS_ID, 0, 256));

        assertEquals(0, values.get(0l).getValue().intValue());
        assertEquals(255, values.get(255l).getValue().intValue());
    }

    private Map<Long, DateValue<Integer>> toMap(Iterator<DateValue<Integer>> iter)
    {
        Map<Long, DateValue<Integer>> copy = new TreeMap<>();
        while (iter.hasNext()) {
            DateValue<Integer> offsetValue = iter.next();
            copy.put(offsetValue.getDate(), offsetValue);
        }
        return copy;
    }

    @Test
    public void testLargeSeries()
    {
        List<DateValue<Integer>> batch = createBatch(10000, 1, 0);
        TimeSeriesHandler<Integer>
                ts = new TimeSeriesHandler<>(10, 1, 0, persistenceHandler);
        ts.batchAdd(TS_ID, batch);

        assertEquals(1000, persistenceHandler.getPersistCount());
        assertEquals(1000, persistenceHandler.getPersistCount());

        Iterator<DateValue<Integer>> iterator = ts.get(TS_ID, 0, 20000);

        int i = 0;
        while(iterator.hasNext())
        {
            DateValue<Integer> tsValue = iterator.next();
            DateValue<Integer> oriValue = batch.get(i++);
            assertEquals(oriValue, tsValue);
        }
    }

    private List<DateValue<Integer>> createBatch(int size, int resolution, int initOffset)
    {
        List<DateValue<Integer>> batch = new ArrayList<>(size);
        for(int i = 0; i < size; i++)
        {
            batch.add(new DateValue<>(initOffset + (i*resolution), initOffset + (i*resolution)));
        }
        return batch;
    }

}