package com.airhacks.enhydrator.transform;

/**
 *
 * @author airhacks.com
 */
public class Mapping {

    private String targetSink;
    private String targetObject;

    public Mapping(String targetSink, String targetObject) {
        this.targetSink = targetSink;
        this.targetObject = targetObject;
    }

    public Mapping() {
    }

    public String getTargetSink() {
        return targetSink;
    }

    public String getTargetObject() {
        return targetObject;
    }
}
