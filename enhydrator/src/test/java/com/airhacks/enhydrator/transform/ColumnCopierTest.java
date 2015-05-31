package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import java.util.Arrays;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ColumnCopierTest {

    private ColumnCopier cut;

    @Before
    public void init() {
        this.cut = new ColumnCopier();
    }

    @Test
    public void executeWithExistingMappings() {
        this.cut.columnMappings.put("duke", Arrays.asList("java", "javaee"));
        Row row = new Row();
        row.addColumn(new Column(0, "duke", "42"));
        Row withCopiedColumns = this.cut.execute(row);
        assertFalse(withCopiedColumns.isEmpty());
        assertThat(withCopiedColumns.getColumns().size(), is(3));
    }

}
