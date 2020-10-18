package org.linkeddatafragments.datasource.index;

import java.util.HashMap;

import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.executionplan.QueryExecutionPlan;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.linkeddatafragments.util.StarString;
import org.rdfhdt.hdt.triples.TripleString;


public class IndexDataSource extends DataSourceBase {

    /**
     * The request processor
     *
     */
    protected final IndexRequestProcessorForTPFs requestProcessor;

    /**
     *
     * @param baseUrl
     * @param datasources
     */
    public IndexDataSource(String baseUrl, HashMap<String, IDataSource> datasources) {
        super("Index", "List of all datasources");
        requestProcessor = new IndexRequestProcessorForTPFs( baseUrl, datasources );
    }

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor)
    {
        return TPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public long cardinalityEstimation(StarString curr) {
        return 0;
    }

    @Override
    public long cardinalityEstimation(TripleString curr) {
        return 0;
    }

    @Override
    public long cardinalityEstimation(StarString curr, QueryExecutionPlan subplan) {
        return 0;
    }


    @Override
    public String getFilename() {
        return null;
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor)
    {
        return requestProcessor;
    }
}
