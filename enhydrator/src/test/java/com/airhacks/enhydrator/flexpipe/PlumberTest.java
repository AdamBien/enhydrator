package com.airhacks.enhydrator.flexpipe;

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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class PlumberTest {

    @Test
    public void writeAndReadConfiguration() {
        Plumber plumber = new Plumber();
        Pipeline origin = PipelineTest.getJDBCPipeline();
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertThat(deserialized, is(origin));
    }

}
