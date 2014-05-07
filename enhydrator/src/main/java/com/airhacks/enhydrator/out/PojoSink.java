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
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javafx.util.Pair;

/**
 *
 * @author airhacks.com
 */
public class PojoSink extends Sink {

    private static final String DEFAULT_NAME = "pojo";

    private Class target;
    private Class childrenType;
    private Consumer<Object> consumer;
    private final String childrenFieldName;

    public PojoSink(Class target, Consumer<Object> consumer) {
        this(DEFAULT_NAME, target, consumer);
    }

    public PojoSink(String sinkName, Class target, Consumer<Object> consumer) {
        super(sinkName);
        this.consumer = consumer;
        this.target = target;
        final Pair<String, Class<? extends Object>> childInfo = getChildInfo(target);
        this.childrenFieldName = childInfo.getKey();
        this.childrenType = childInfo.getValue();
    }

    @Override
    public void processRow(Row currentRow) {
        Object targetObject = convert(this.target, currentRow);
        if (currentRow.hasChildren()) {
            mapChildren(targetObject, currentRow.getChildren());
        }
        this.consumer.accept(targetObject);
    }

    Object convert(Class pojoType, Row currentRow) {
        Object targetObject = newInstance(pojoType);
        currentRow.getColumnValues().forEach((k, v) -> setField(targetObject, k, v));
        return targetObject;
    }

    void mapChildren(Object parent, List<Row> children) {
        List<Object> pojos = children.stream().
                map(c -> convert(this.childrenType, c)).
                collect(Collectors.toList());
        setField(parent, this.childrenFieldName, pojos);
    }

    Object newInstance(Class clazz) throws IllegalStateException {
        Object targetObject;
        try {
            targetObject = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new IllegalStateException("Cannot instantiate: " + this.target.getName(), ex);
        }
        return targetObject;
    }

    public static void setField(Object target, String name, Object value) {
        Objects.requireNonNull(target, "Object cannot be null");
        Class<? extends Object> targetClass = target.getClass();
        Field field;
        try {
            field = targetClass.getDeclaredField(name);
            field.setAccessible(true);
        } catch (NoSuchFieldException ex) {
            throw new IllegalArgumentException(target.getClass() + " does not have a field with the name " + name, ex);
        } catch (SecurityException ex) {
            throw new IllegalStateException("Cannot access private field", ex);
        }
        try {
            field.set(target, value);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException("Cannot set field: " + name + " with " + value, ex);
        } finally {
            field.setAccessible(false);
        }
    }

    static Pair<String, Class<? extends Object>> getChildInfo(Class target) {
        Field[] declaredFields = target.getDeclaredFields();
        for (Field field : declaredFields) {
            final Class<?> type = field.getType();
            if (type.isAssignableFrom(Collection.class)) {
                final ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
                String clazz = null;
                try {
                    clazz = parameterizedType.getActualTypeArguments()[0].getTypeName();
                    return new Pair(field.getName(), Class.forName(clazz));
                } catch (ClassNotFoundException ex) {
                    throw new IllegalStateException("Cannot find class " + clazz, ex);
                }
            }
        }
        return null;
    }

}
