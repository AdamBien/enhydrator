package com.airhacks.enhydrator.out;

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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class PojoSinkTest {

    PojoSink cut;

    CachingConsumer cachingConsumer;

    @Before
    public void init() {
        this.cachingConsumer = new CachingConsumer();
        this.cut = new PojoSink(Developer.class, this.cachingConsumer);
    }

    @Test
    public void stringMapping() {
        final String expected = "duke";
        Row row = new Row();
        row.addColumn(-1, "name", expected);
        this.cut.processRow(row);
        Developer dev = getDeveloper();
        String actual = dev.getName();
        assertThat(actual, is(expected));
    }

    @Test
    public void intMapping() {
        final int expected = 42;
        Row row = new Row();
        row.addColumn(-1, "age", expected);
        this.cut.processRow(row);
        Developer dev = getDeveloper();
        int actual = dev.getAge();
        assertThat(actual, is(expected));
    }

    @Test
    public void doubleMapping() {
        final double expected = 1.5;
        Row row = new Row();
        row.addColumn(-1, "weight", expected);
        this.cut.processRow(row);
        Developer dev = getDeveloper();
        double actual = dev.getWeight();
        assertThat(actual, is(expected));
    }

    @Test(expected = IllegalArgumentException.class)
    public void typeMismatch() {
        final double expected = 1.5;
        Row row = new Row();
        //name is String
        row.addColumn(-1, "name", expected);
        this.cut.processRow(row);
    }

    @Test(expected = IllegalArgumentException.class)
    public void notExistingField() {
        final double expected = 1.5;
        Row row = new Row();
        row.addColumn(-1, "SHOULD-NOT-EXIST", expected);
        this.cut.processRow(row);
    }

    @Test
    public void pojoWithRelation() {
        final String expected = "duke";
        Row parent = new Row();
        parent.addColumn(-1, "name", expected);

        final int expectedRanking = 2;
        final String expectedLanguageName = "java";

        Row programming = new Row();
        programming.addColumn(-1, "name", expectedLanguageName);
        programming.addColumn(-1, "ranking", expectedRanking);
        parent.add(programming);
        this.cut.processRow(parent);

        Developer developer = getDeveloper();
        assertNotNull(developer);
        assertThat(developer.getLanguages().size(), is(1));

        ProgrammingLanguage language = developer.getLanguages().iterator().next();
        assertThat(language.getName(), is(expectedLanguageName));
        assertThat(language.getRanking(), is(expectedRanking));
    }

    @Test
    public void pojoWithoutRelation() {
        CachingConsumer consumer = new CachingConsumer();
        PojoSink sink = new PojoSink(DeveloperWithoutKids.class, consumer);
        final String expected = "duke";
        Row parent = new Row();
        parent.addColumn(-1, "name", expected);

        final int expectedRanking = 2;
        final String expectedLanguageName = "java";

        Row programming = new Row();
        programming.addColumn(-1, "name", expectedLanguageName);
        programming.addColumn(-1, "ranking", expectedRanking);
        parent.add(programming);
        sink.processRow(parent);
        DeveloperWithoutKids kidless = (DeveloperWithoutKids) consumer.getObject();
        assertNotNull(kidless);
    }

    @Test
    public void pojoWithAnnotatedField() {
        CachingConsumer consumer = new CachingConsumer();
        PojoSink sink = new PojoSink(DeveloperWithAnnotatedField.class, consumer);

        final String expectedName = "duke";
        final int expectedAge = 42;

        Row programming = new Row();
        programming.addColumn(-1, "name", expectedName);
        programming.addColumn(-1, "age", expectedAge);
        sink.processRow(programming);
        DeveloperWithAnnotatedField kidless = (DeveloperWithAnnotatedField) consumer.getObject();
        assertNotNull(kidless);
    }

    @Test(expected = IllegalStateException.class)
    public void pojoWithTooManyRelations() {
        new PojoSink(DeveloperWithTooManyRelations.class, this.cachingConsumer);
    }

    @Test
    public void getChildInfo() {
        Class<? extends Object> childType = PojoSink.getChildInfo(Developer.class).getValue();
        assertNotNull(childType);
        System.out.println("Childtype: " + childType);
        assertTrue(ProgrammingLanguage.class.isAssignableFrom(childType));
    }

    private Developer getDeveloper() {
        return (Developer) this.cachingConsumer.getObject();
    }

}
