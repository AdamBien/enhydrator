package com.airhacks.enhydrator.out;

import java.util.function.Consumer;

/**
 *
 * @author airhacks.com
 */
public class CachingConsumer implements Consumer<Object> {

    private Object object;

    @Override
    public void accept(Object object) {
        this.object = object;
    }

    public Object getObject() {
        return object;
    }
}
