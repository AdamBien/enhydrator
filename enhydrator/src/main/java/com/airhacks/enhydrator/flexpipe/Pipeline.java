package com.airhacks.enhydrator.flexpipe;

import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import java.util.List;

/**
 *
 * @author airhacks.com
 */
public interface Pipeline {

    String getName();

    List<EntryTransformation> getEntryTransformations();

    List<String> getExpressions();

    List<String> getPostRowTransfomers();

    List<String> getPreRowTransformers();

    List<Object> getQueryParams();

    Sink getSink();

    String getScriptsHome();

    JDBCSource getSource();

    String getSqlQuery();

}
