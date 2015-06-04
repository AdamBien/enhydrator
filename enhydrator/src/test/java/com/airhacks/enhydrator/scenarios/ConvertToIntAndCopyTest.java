package com.airhacks.enhydrator.scenarios;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.transform.ColumnCopier;
import com.airhacks.enhydrator.transform.Datatype;
import com.airhacks.enhydrator.transform.DatatypeMapper;
import com.airhacks.enhydrator.transform.Memory;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ConvertToIntAndCopyTest {

    private VirtualSinkSource input;
    private VirtualSinkSource output;
    private Pump pump;

    @Before
    public void init() {
        this.input = new VirtualSinkSource();
        this.output = new VirtualSinkSource();
        //map column answer to Integer
        final DatatypeMapper datatypeMapper = new DatatypeMapper();
        datatypeMapper.addMapping("answer", Datatype.INTEGER);
        //Copy column question to answer and origin
        ColumnCopier columnCopier = new ColumnCopier();
        columnCopier.addMapping("question", "answer", "origin");
        this.pump = new Pump.Engine().
                startWith(columnCopier).
                startWith(datatypeMapper).
                with("answer", (c) -> {
                    Integer columnValue = (Integer) c;
                    return columnValue * 2;
                }).
                from(input).
                to(output).
                to(new LogSink()).
                build();
    }

    @Test
    public void scenario() {
        Row row = new Row();
        row.addColumn(new Column(0, "question", "21"));
        this.input.getRows().add(row);
        Memory result = this.pump.start();
        assertThat(result.getErroneousRowCount(), is(0l));
        assertThat(result.getProcessedRowCount(), is(1l));
        Row first = this.output.getRow(0);
        assertThat(first.getColumns().size(), is(3));

        Column answerColumn = first.getColumnByName("answer");
        assertThat(answerColumn.getValue(), is(42));

        Column originColumn = first.getColumnByName("origin");
        assertThat(originColumn.getValue(), is("21"));

        Column questionColumn = first.getColumnByName("question");
        assertThat(questionColumn.getValue(), is("21"));
    }

}
