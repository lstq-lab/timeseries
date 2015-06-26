package wa.timeseries.core;

import org.junit.Before;
import org.junit.Test;
import wa.timeseries.core.persistence.InMemoryTimeSeriesPersistenceHandler;

import java.util.*;

import static org.junit.Assert.*;

public class TimeSeriesTest {

    private InMemoryTimeSeriesPersistenceHandler<Integer> persistenceHandler;

    @Before
    public void before()
    {
        persistenceHandler = new InMemoryTimeSeriesPersistenceHandler<>();
    }

    @Test
    public void testAdd() {

        TimeSeries<Integer> ts = new TimeSeries<>(1, 0, persistenceHandler);
        ts.add(0, 0);
        assertEquals(0, (int)ts.get(0).get());
        ts.add(1, 1);
        assertEquals(1, (int)ts.get(1).get());

        assertEquals(1, persistenceHandler.getSlices().size());
    }

    @Test
    public void testAddSecondSlice() {
        TimeSeries<Integer> ts = new TimeSeries<>(1, 0, persistenceHandler);

        ts.add(0, 0);
        assertEquals(0, (int) ts.get(0).get());
        ts.add(256, 256);
        assertEquals(256, (int) ts.get(256).get());

        assertEquals(2, persistenceHandler.getSlices().size());

        ts.add(257, 257);
        assertEquals(257, (int) ts.get(257).get());
        ts.add(258, 258);
        assertEquals(258, (int) ts.get(258).get());

        assertEquals(2, persistenceHandler.getSlices().size());

        ts.add(512, 512);
        assertEquals(512, (int) ts.get(512).get());
        assertEquals(3, persistenceHandler.getSlices().size());
    }

    @Test
    public void testDataValueIterator()
    {
        Collection<DateValue<Integer>> batch = createBatch(10000, 5, 0);
        TimeSeries<Integer> ts = new TimeSeries<>(10, 5, 0, persistenceHandler);
        ts.batchAdd(batch);

        Map<Long, DateValue<Integer>> values = toMap(ts.get(0, 256));

        assertEquals(0, values.get(0l).getValue().intValue());
        assertEquals(255, values.get(255l).getValue().intValue());
    }

    @Test
    public void testDataValueIterator1()
    {
        Collection<DateValue<Integer>> batch = createBatch(10000, 1, 0);
        TimeSeries<Integer> ts = new TimeSeries<>(10, 1, 0, persistenceHandler);
        ts.batchAdd(batch);

        Map<Long, DateValue<Integer>> values = toMap(ts.get(0, 256));

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
        TimeSeries<Integer> ts = new TimeSeries<>(10, 1, 0, persistenceHandler);
        ts.batchAdd(batch);

        assertEquals(1000, persistenceHandler.getPersistCount());
        assertEquals(1000, persistenceHandler.getPersistCount());

        Iterator<DateValue<Integer>> iterator = ts.get(0, 20000);

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