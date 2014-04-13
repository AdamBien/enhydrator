package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class FunctionScriptLoaderTest {

    FunctionScriptLoader cut;

    @Before
    public void init() {
        this.cut = new FunctionScriptLoader();
    }

    @Test
    public void scriptLoadingWorks() throws Exception {
        Entry entry = new Entry(0, "chief", 42, "duke");
        EntryTransformer function = this.cut.getEntryTransformer();
        assertNotNull(function);
        List<Entry> transformedEntries = function.execute(entry, new ArrayList());
        assertThat(transformedEntries, hasItem(entry));

    }

}
