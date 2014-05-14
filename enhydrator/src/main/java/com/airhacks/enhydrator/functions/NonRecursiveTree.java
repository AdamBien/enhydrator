package com.airhacks.enhydrator.functions;

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
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.transform.RowTransformer;
import java.util.Objects;

/**
 *
 * @author airhacks.com
 */
public class NonRecursiveTree implements RowTransformer {

    private Row root;

    private final String parentIndicator;
    private final String idColumn;

    public NonRecursiveTree(String idColumn, String parentIndicator) {
        Objects.requireNonNull(parentIndicator, "Column for parent indication cannot be null");
        Objects.requireNonNull(idColumn, "ID Column cannot be null");
        this.parentIndicator = parentIndicator;
        this.idColumn = idColumn;
    }

    @Override
    public Row execute(Row input) {
        if (input == null) {
            return null;
        }
        if (isParent(input)) {
            this.root = input;
            return root;
        } else {
            root.add(input);
            return null;
        }
    }

    boolean isParent(Row input) {
        Object idValue = input.getColumnValue(idColumn);
        Object parentValue = input.getColumnValue(this.parentIndicator);
        return parentValue != null && idValue.equals(parentValue);
    }
}
