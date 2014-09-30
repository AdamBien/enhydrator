package com.airhacks.samples.json;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.ScriptableSource;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.out.LogSink;
import java.io.FileNotFoundException;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class FromJsonToObjectScenarioTest {

    private final static String INPUT = "./src/test/resources/";

    @Test
    public void fromJSONToLog() throws FileNotFoundException {
        Source source = new ScriptableSource(INPUT + "languages.json",
                INPUT + "converter.js", "UTF-8");

        Pump pump = new Pump.Engine().
                from(source).
                to(new LogSink()).
                build();
        pump.start();
    }

}
