package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class FunctionScriptLoaderTest {

    FunctionScriptLoader cut;
    public static final String SCRIPTS_HOME_FOLDER = "./src/test/scripts";

    @Before
    public void init() {
        this.cut = new FunctionScriptLoader(SCRIPTS_HOME_FOLDER);
    }

    @Test
    public void entryTransfomerLoadingAndExecution() throws Exception {
        Entry entry = new Entry(0, "chief", 42, "duke");
        EntryTransformer function = this.cut.getEntryTransformer("noop");
        assertNotNull(function);
        List<Entry> transformedEntries = function.execute(entry, new ArrayList());
        assertThat(transformedEntries, hasItem(entry));
    }

    @Test
    public void rowTransfomerLoadingAndExecution() throws Exception {
        Entry entry = new Entry(0, "chief", 42, "duke");
        List<Entry> input = new ArrayList<>();
        input.add(entry);
        RowTransformer function = this.cut.getRowTransformer("noop");
        assertNotNull(function);
        List<Entry> transformedEntries = function.execute(input);
        assertThat(transformedEntries, is(input));
    }

    @Test
    public void load() {
        String content = this.cut.load("entry", "noop");
        assertNotNull(content);
    }

}
