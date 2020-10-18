package org.linkeddatafragments.datasource;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.spf.IStarPatternElement;
import org.linkeddatafragments.fragments.spf.IStarPatternFragmentRequest;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragment;
import org.linkeddatafragments.fragments.tpf.TriplePatternFragmentImpl;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.linkeddatafragments.util.Tuple;

import java.util.List;


public abstract class
    AbstractRequestProcessorForTriplePatterns<CTT,NVT,AVT>
        extends AbstractRequestProcessor
{

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected final Worker<CTT,NVT,AVT> getWorker(
            final ILinkedDataFragmentRequest request )
                                                throws IllegalArgumentException
    {
        if ( request instanceof ITriplePatternFragmentRequest<?,?,?> ) {
            @SuppressWarnings("unchecked")
            final ITriplePatternFragmentRequest<CTT,NVT,AVT> tpfRequest =
                      (ITriplePatternFragmentRequest<CTT,NVT,AVT>) request;
            return getTPFSpecificWorker( tpfRequest );
        }
        else
            throw new IllegalArgumentException( request.getClass().getName() );
    }

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    abstract protected Worker<CTT,NVT,AVT> getTPFSpecificWorker(
            final ITriplePatternFragmentRequest<CTT,NVT,AVT> request )
                    throws IllegalArgumentException;

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    abstract static protected class Worker<CTT,NVT,AVT>
        extends AbstractRequestProcessor.Worker
    {

        /**
         *
         * @param request
         */
        public Worker(
                 final ITriplePatternFragmentRequest<CTT,NVT,AVT> request )
        {
            super( request );
        }

        @Override
        public long estimateCardinality() throws IllegalArgumentException {
            @SuppressWarnings("unchecked")
            final ITriplePatternFragmentRequest<CTT,NVT,AVT> tpfRequest =
                    (ITriplePatternFragmentRequest<CTT,NVT,AVT>) request;

            return estimateCardinality( tpfRequest.getSubject(),
                    tpfRequest.getPredicate(),
                    tpfRequest.getObject());
        }

        abstract protected long estimateCardinality(
                final ITriplePatternElement<CTT,NVT,AVT> subj,
                final ITriplePatternElement<CTT,NVT,AVT> pred,
                final ITriplePatternElement<CTT,NVT,AVT> obj)
                throws IllegalArgumentException;

        @Override
        public double meanElementSize() throws IllegalArgumentException {
            @SuppressWarnings("unchecked")
            final ITriplePatternFragmentRequest<CTT,NVT,AVT> tpfRequest =
                    (ITriplePatternFragmentRequest<CTT,NVT,AVT>) request;

            return meanElementSize(tpfRequest.getSubject(),
                    tpfRequest.getPredicate(),
                    tpfRequest.getObject());
        }

        abstract protected double meanElementSize(
                final ITriplePatternElement<CTT,NVT,AVT> subj,
                final ITriplePatternElement<CTT,NVT,AVT> pred,
                final ITriplePatternElement<CTT,NVT,AVT> obj) throws IllegalArgumentException;

        /**
         *
         * @return
         * @throws IllegalArgumentException
         */
        @Override
        public ILinkedDataFragment createRequestedFragment()
                                                throws IllegalArgumentException
        {
            final long limit = ILinkedDataFragmentRequest.TRIPLESPERPAGE;
            final long offset;
            if ( request.isPageRequest() )
                offset = limit * ( request.getPageNumber() - 1L );
            else
                offset = 0L;

            @SuppressWarnings("unchecked")
            final ITriplePatternFragmentRequest<CTT,NVT,AVT> tpfRequest =
                      (ITriplePatternFragmentRequest<CTT,NVT,AVT>) request;

            return createFragment( tpfRequest.getSubject(),
                                   tpfRequest.getPredicate(),
                                   tpfRequest.getObject(),
                                   offset, limit );
        }

        /**
         *
         * @param subj
         * @param pred
         * @param obj
         * @param offset
         * @param limit
         * @return
         * @throws IllegalArgumentException
         */
        abstract protected ILinkedDataFragment createFragment(
                            final ITriplePatternElement<CTT,NVT,AVT> subj,
                            final ITriplePatternElement<CTT,NVT,AVT> pred,
                            final ITriplePatternElement<CTT,NVT,AVT> obj,
                            final long offset,
                            final long limit )
                                               throws IllegalArgumentException;


        @Override
        public ILinkedDataFragment createRequestedMetadataFragment()
                throws IllegalArgumentException
        {
            return createEmptyTriplePatternFragment();
        }

        /**
         *
         * @return
         */
        protected ITriplePatternFragment createEmptyTriplePatternFragment()
        {
            return new TriplePatternFragmentImpl( request.getFragmentURL(),
                                                  request.getDatasetURL() );
        }

        /**
         *
         * @param triples
         * @param totalSize
         * @param isLastPage
         * @return
         */
        protected ITriplePatternFragment createTriplePatternFragment(
                                                     final Model triples,
                                                     final long totalSize,
                                                     final boolean isLastPage )
        {
            return new TriplePatternFragmentImpl( triples,
                                                  totalSize,
                                                  request.getFragmentURL(),
                                                  request.getDatasetURL(),
                                                  request.getPageNumber(),
                                                  isLastPage );
        }

        protected ITriplePatternFragment createTriplePatternFragment(
                final Model triples,
                final long totalSize,
                final boolean isLastPage,
                final int found )
        {
            return new TriplePatternFragmentImpl( triples,
                    totalSize,
                    request.getFragmentURL(),
                    request.getDatasetURL(),
                    request.getPageNumber(),
                    isLastPage,
                    found);
        }

    } // end of class Worker

}
