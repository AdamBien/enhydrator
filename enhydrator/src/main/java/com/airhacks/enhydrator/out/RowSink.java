package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Row;
import java.util.function.Consumer;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "row-sink")
public class RowSink extends Sink {

    @XmlTransient
    private Consumer<Row> consumer;

    public RowSink() {
        super("*");
    }

    public RowSink(String name, Consumer<Row> consumer) {
        super(name);
        this.consumer = consumer;
    }

    @Override
    public void processRow(Row entries) {
        consumer.accept(entries);
    }

}
