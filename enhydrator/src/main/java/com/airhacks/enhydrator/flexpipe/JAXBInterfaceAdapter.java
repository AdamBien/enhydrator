package com.airhacks.enhydrator.flexpipe;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 *
 * @author airhacks.com
 */
public class JAXBInterfaceAdapter extends XmlAdapter<Object, Object> {

    @Override
    public Object unmarshal(Object v) throws Exception {
        return v;
    }

    @Override
    public Object marshal(Object v) throws Exception {
        return v;
    }

}
