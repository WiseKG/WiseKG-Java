package org.linkeddatafragments.datasource;

import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;


abstract public class AbstractRequestProcessor
    implements IFragmentRequestProcessor
{
    @Override
    public void close() {}

    /**
     * Create an {@link ILinkedDataFragment} from {@link ILinkedDataFragmentRequest}
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    final public ILinkedDataFragment createRequestedFragment(
            final ILinkedDataFragmentRequest request )
                    throws IllegalArgumentException
    {
        return getWorker( request ).createRequestedFragment();
    }

    @Override
    final public ILinkedDataFragment createRequestedMetadataFragment(
            final ILinkedDataFragmentRequest request )
            throws IllegalArgumentException
    {
        return getWorker( request ).createRequestedMetadataFragment();
    }

    @Override
    public long estimateCardinality(ILinkedDataFragmentRequest request) throws IllegalArgumentException {
        return getWorker( request ).estimateCardinality();
    }

    @Override
    public double meanElementSize(ILinkedDataFragmentRequest request) throws IllegalArgumentException {
        return getWorker( request ).meanElementSize();
    }

    /**
     * Get the {@link Worker} from {@link ILinkedDataFragmentRequest}
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    abstract protected Worker getWorker(
            final ILinkedDataFragmentRequest request )
                    throws IllegalArgumentException;

    /**
     * Processes {@link ILinkedDataFragmentRequest}s
     *
     */
    abstract static protected class Worker
    {

        /**
         * The  {@link ILinkedDataFragmentRequest} to process
         */
        public final ILinkedDataFragmentRequest request;

        /**
         * Create a Worker
         *
         * @param request
         */
        public Worker( final ILinkedDataFragmentRequest request )
        {
            this.request = request;
        }

        /**
         * Create the requested {@link ILinkedDataFragment}
         *
         * @return The ILinkedDataFragment
         * @throws IllegalArgumentException
         */
        abstract public ILinkedDataFragment createRequestedFragment()
                                               throws IllegalArgumentException;

        abstract public ILinkedDataFragment createRequestedMetadataFragment()
                throws IllegalArgumentException;

        abstract public double meanElementSize()
                                               throws IllegalArgumentException;

        abstract public long estimateCardinality()
                                               throws IllegalArgumentException;

    } // end of class Worker

}
