package org.linkeddatafragments.datasource.tdb;

import java.io.File;

import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.executionplan.QueryExecutionPlan;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.linkeddatafragments.util.StarString;
import org.rdfhdt.hdt.triples.TripleString;


public class JenaTDBDataSource extends DataSourceBase {

    /**
     * The request processor
     *
     */
    protected final JenaTDBBasedRequestProcessorForTPFs requestProcessor;

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor)
    {
        return TPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor)
    {
        return requestProcessor;
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

    /**
     * Constructor
     *
     * @param title
     * @param description
     * @param tdbdir directory used for TDB backing
     */
    public JenaTDBDataSource(String title, String description, File tdbdir) {
        super(title, description);
        requestProcessor = new JenaTDBBasedRequestProcessorForTPFs( tdbdir );
    }
}
