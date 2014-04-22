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
package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author airhacks.com
 */
public class FilterExpressionTest {

    FilterExpression fe;

    @Before
    public void init() {
        this.fe = new FilterExpression();
    }

    @Test
    public void accept() {
        Boolean result = this.fe.execute(new ArrayList<>(), "true");
        assertTrue(result);

    }

    @Test
    public void drop() {
        Boolean result = this.fe.execute(new ArrayList<>(), "false");
        assertFalse(result);
    }

    @Test
    public void wrongReturnTypeIsFalse() {
        Boolean result = this.fe.execute(new ArrayList<>(), "'hey'");
        assertFalse(result);
    }

}
