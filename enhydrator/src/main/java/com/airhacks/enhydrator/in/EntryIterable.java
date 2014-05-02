/*
 * Copyright 2014 Adam Bien.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.airhacks.enhydrator.in;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.function.Function;

/**
 *
 * @author airhacks.com
 */
public class EntryIterable implements Iterable<Row> {

    private final ResultSetIterator resultSetIterator;
    private final Function<ResultSet, Row> rowTransformer;

    public EntryIterable(ResultSetIterator resultSetIterator) {
        this.resultSetIterator = resultSetIterator;
        this.rowTransformer = new ResultSetToEntries();
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {

            @Override
            public boolean hasNext() {
                return resultSetIterator.hasNext();
            }

            @Override
            public Row next() {
                ResultSet rs = resultSetIterator.next();
                return rowTransformer.apply(rs);
            }
        };
    }

}
