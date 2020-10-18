package org.linkeddatafragments.datasource;

import java.io.Closeable;

import org.linkeddatafragments.executionplan.QueryExecutionPlan;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.util.StarString;
import org.rdfhdt.hdt.triples.TripleString;

public interface IDataSource extends Closeable {

    /**
     *
     * @return
     */
    public String getTitle();

    /**
     *
     * @return
     */
    public String getDescription();

    /**
     * Returns a data source specific {@link IFragmentRequestParser}.
     * @return
     */
    IFragmentRequestParser getRequestParser(ProcessorType processor);

    /**
     * Returns a data source specific {@link IFragmentRequestProcessor}.
     * @return
     */
    IFragmentRequestProcessor getRequestProcessor(ProcessorType processor);

    long cardinalityEstimation(StarString curr);
    long cardinalityEstimation(StarString curr, QueryExecutionPlan subplan);
    long cardinalityEstimation(TripleString curr);
    String getFilename();

    public enum ProcessorType {
        TPF, BRTPF, SPF
    }
}
