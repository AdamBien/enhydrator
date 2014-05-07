package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Row;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.function.Consumer;

/**
 *
 * @author airhacks.com
 */
public class PojoSink extends Sink {

    private static final String DEFAULT_NAME = "pojo";

    private Class target;
    private Consumer<Object> consumer;

    public PojoSink(Class target, Consumer<Object> consumer) {
        this(DEFAULT_NAME, target, consumer);
    }

    public PojoSink(String sinkName, Class target, Consumer<Object> consumer) {
        super(sinkName);
        this.consumer = consumer;
        this.target = target;
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

    @Override
    public void processRow(Row entry) {
        Object targetObject;
        try {
            targetObject = this.target.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new IllegalStateException("Cannot instantiate: " + this.target.getName(), ex);
        }
        entry.getColumnValues().forEach((k, v) -> setField(targetObject, k, v));
        this.consumer.accept(targetObject);
    }

}
