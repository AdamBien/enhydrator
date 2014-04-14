package com.airhacks.enhydrator.out;

/**
 *
 * @author airhacks.com
 */
public class Sinks {

    public static Sink systemOut() {
        return new SystemOutSink();
    }

}
