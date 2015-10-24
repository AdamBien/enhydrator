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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "pojo-sink")
public class PojoSink extends NamedSink {

    private static final String DEFAULT_NAME = "pojo";
    @XmlTransient
    protected Class target;
    @XmlTransient
    protected Class childrenType;
    @XmlTransient
    protected Consumer<Object> consumer;

    @XmlTransient
    protected String childrenFieldName = null;

    @XmlTransient
    protected final Consumer<Map<String, Object>> devNullConsumer;

    @XmlTransient
    protected Map<String, Object> unmappedFields;

    public PojoSink(Class target, Consumer<Object> consumer, Consumer<Map<String, Object>> devNull) {
        this(DEFAULT_NAME, target, consumer, devNull);
    }

    public PojoSink(String sinkName, Class target, Consumer<Object> consumer, Consumer<Map<String, Object>> devNull) {
        super(sinkName);
        this.consumer = consumer;
        this.target = target;
        this.devNullConsumer = devNull;
        initializeChildren();
    }

    void initializeChildren() {
        final Pair<String, Class<? extends Object>> childInfo = getChildInfo(this.target);
        if (childInfo != null) {
            this.childrenFieldName = childInfo.getKey();
            this.childrenType = childInfo.getValue();
        }
        checkConventions(this.target);
    }

    static void checkConventions(Class parent) {
        Field[] declaredFields = parent.getDeclaredFields();
        int counter = 0;
        StringJoiner joiner = new StringJoiner(",");
        for (Field field : declaredFields) {
            final Class<?> type = field.getType();
            if (type.isAssignableFrom(Collection.class)) {
                joiner.add(field.getName());
                counter++;
            }
        }
        if (counter > 1) {
            throw new IllegalStateException("Multiple (" + counter + ") collection fields with names " + joiner.toString() + "");
        }
    }

    @Override
    public void processRow(Row currentRow) {
        this.unmappedFields = new HashMap<>();
        Object targetObject = convert(this.target, currentRow);
        if (currentRow.hasChildren() && this.childrenType != null) {
            mapChildren(targetObject, currentRow.getChildren());
        }
        this.consumer.accept(targetObject);
        if (!this.unmappedFields.isEmpty() && this.devNullConsumer != null) {
            this.devNullConsumer.accept(unmappedFields);
        }
    }

    protected Object convert(Class pojoType, Row currentRow) {
        Object targetObject = newInstance(pojoType);
        currentRow.getColumnValues().forEach((k, v) -> setField(targetObject, k, v));
        return targetObject;
    }

    protected void mapChildren(Object parent, List<Row> children) {
        List<Object> pojos = children.stream().
                map(c -> convert(this.childrenType, c)).
                collect(Collectors.toList());
        setField(parent, this.childrenFieldName, Optional.of(pojos));
    }

    protected Object newInstance(Class clazz) throws IllegalStateException {
        Object targetObject;
        try {
            targetObject = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new IllegalStateException("Cannot instantiate: " + this.target.getName(), ex);
        }
        return targetObject;
    }

    public void setField(Object target, String name, Optional<Object> value) {
        Objects.requireNonNull(target, "Object cannot be null");
        Class<? extends Object> targetClass = target.getClass();
        Field field;
        try {
            field = targetClass.getDeclaredField(name);
        } catch (NoSuchFieldException ex) {
            field = getFieldAnnotatedWith(targetClass, name);
            if (field == null) {
                if (this.devNullConsumer != null) {
                    unmappedFields.put(name, value.orElse(null));
                    return;
                } else {
                    throw new IllegalArgumentException(target.getClass() + " does not have a field with the name " + name, ex);
                }
            }
        } catch (SecurityException ex) {
            throw new IllegalStateException("Cannot access private field", ex);
        }
        if(value.isPresent()) {
            try {
                field.setAccessible(true);
                field.set(target, value.get());
            } catch (IllegalAccessException ex) {
                throw new IllegalStateException("Cannot set field: " + name + " with " + value.get(), ex);
            } finally {
                field.setAccessible(false);
            }
        }
    }

    public static Field getFieldAnnotatedWith(Class clazz, String name) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            ColumnName columnName = field.getAnnotation(ColumnName.class);
            if (columnName != null) {
                String customName = columnName.value();
                if (customName.equals(name)) {
                    return field;
                }
            }
        }
        return null;
    }

    protected static Pair<String, Class<? extends Object>> getChildInfo(Class target) {
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
