package wa.timeseries.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SeriesSliceTest {

    @Test
    public void testIterator()
    {
        SeriesSlice<Integer> slice = new SeriesSlice<>(1, 100, 5);
        slice.add(0, 10);
        assertEquals(500, slice.iterator().next().getOffset());
    }

    @Test
    public void testAdd() {
        SeriesSlice<Integer> slice = new SeriesSlice<>(0, 100, 5);
        assertNull(slice.add(0, 0));

        Integer value = slice.add(1, 1);
        assertNotNull("The value should collide with the previous offset", value);
        assertEquals((Integer) 0, value);

        value = slice.add(5, 5);
        assertNull(value);

        value = slice.add(100, 100);
        assertNull(value);
    }

    @Test(expected = RuntimeException.class)
    public void testAddOutsideSlice() {
        SeriesSlice<Integer> slice = new SeriesSlice<>(0, 100, 1);
        slice.add(101, 100);
    }

    @Test
    public void testAddMinResolution() {
        SeriesSlice<Integer> slice = new SeriesSlice<>(0, 100, 1);
        assertNull(slice.add(1, 1));
        assertNull(slice.add(2, 2));
        assertNull(slice.add(3, 3));
        assertNull(slice.add(4, 4));

        assertEquals((Integer) 4, slice.get(4));

        assertEquals((Integer) 2, slice.add(2, 20));
        assertEquals((Integer) 20, slice.get(2));
    }
}
