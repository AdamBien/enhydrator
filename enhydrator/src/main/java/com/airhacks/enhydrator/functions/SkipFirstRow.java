package com.airhacks.enhydrator.functions;

import com.airhacks.enhydrator.in.Row;
import java.util.function.Function;

/**
 *
 * @author airhacks.com
 */
public class SkipFirstRow implements Function<Row, Row> {

    private boolean skipped = false;

    @Override
    public Row apply(Row input) {
        if (skipped) {
            return input;
        } else {
            skipped = true;
            return null;
        }

    }

}
