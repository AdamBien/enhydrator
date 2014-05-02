package com.airhacks.enhydrator.functions;

import com.airhacks.enhydrator.in.Row;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class SkipFirstRowTest {

    @Test
    public void skipFirstRow() {
        SkipFirstRow cut = new SkipFirstRow();
        Row actual = cut.apply(new Row());
        assertNull(actual);
        actual = cut.apply(new Row());
        assertNotNull(actual);
    }

}
