package org.linkeddatafragments.datasource.hdt;

import java.io.IOException;
import java.util.Iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.datasource.AbstractRequestProcessorForTriplePatterns;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;




public class HdtBasedRequestProcessorForTPFs
    extends AbstractRequestProcessorForTriplePatterns<RDFNode,String,String>
{

    /**
     * HDT Datasource
     */
    protected final HDT datasource;

    /**
     * The dictionary
     */
    protected final NodeDictionary dictionary;

    /**
     * Creates the request processor.
     *
     * @throws IOException if the file cannot be loaded
     */
    public HdtBasedRequestProcessorForTPFs( HDT hdt, NodeDictionary dict )
    {
        datasource = hdt;
        dictionary = dict;
    }

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getTPFSpecificWorker(
            final ITriplePatternFragmentRequest<RDFNode,String,String> request )
                                                throws IllegalArgumentException
    {
        return new Worker( request );
    }

    /**
     * Worker class for HDT
     */
    protected class Worker
       extends AbstractRequestProcessorForTriplePatterns.Worker<RDFNode,String,String>
    {

        /**
         * Create HDT Worker
         *
         * @param req
         */
        public Worker(
                final ITriplePatternFragmentRequest<RDFNode,String,String> req )
        {
            super( req );
        }

        @Override
        protected long estimateCardinality(ITriplePatternElement<RDFNode, String, String> subject,
                                           ITriplePatternElement<RDFNode, String, String> predicate,
                                           ITriplePatternElement<RDFNode, String, String> object)
                                                                        throws IllegalArgumentException {
            int subjectId = subject.isVariable() ? 0 : dictionary.getIntID(subject.asConstantTerm().asNode(), TripleComponentRole.SUBJECT);
            int predicateId = predicate.isVariable() ? 0 : dictionary.getIntID(predicate.asConstantTerm().asNode(), TripleComponentRole.PREDICATE);
            int objectId = object.isVariable() ? 0 : dictionary.getIntID(object.asConstantTerm().asNode(), TripleComponentRole.OBJECT);

            if (subjectId < 0 || predicateId < 0 || objectId < 0) {
                return 0;
            }
            IteratorTripleID matches = datasource.getTriples().search(new TripleID(subjectId, predicateId, objectId));
            return matches.estimatedNumResults();
        }

        @Override
        protected double meanElementSize(ITriplePatternElement<RDFNode, String, String> subj,
                                         ITriplePatternElement<RDFNode, String, String> pred,
                                         ITriplePatternElement<RDFNode, String, String> obj) throws IllegalArgumentException {
            long total = 0;
            int num = 0;

            String subject = subj.isVariable() ? "" : subj.asConstantTerm().toString();
            if(!subject.equals("")) {
                total += subject.getBytes().length;
                num++;
            }

            String predicate = pred.isVariable() ? "" : pred.asConstantTerm().toString();
            if(!predicate.equals("")) {
                total += predicate.getBytes().length;
                num++;
            }

            String object = obj.isVariable() ? "" : obj.asConstantTerm().toString();
            if(!object.equals("")) {
                total += object.getBytes().length;
                num++;
            }

            return (double) total / (double) num;
        }

        /**
         * Creates an {@link ILinkedDataFragment} from the HDT
         *
         * @param subject
         * @param predicate
         * @param object
         * @param offset
         * @param limit
         * @return
         */
        @Override
        protected ILinkedDataFragment createFragment(
                   final ITriplePatternElement<RDFNode,String,String> subject,
                   final ITriplePatternElement<RDFNode,String,String> predicate,
                   final ITriplePatternElement<RDFNode,String,String> object,
                   final long offset,
                   final long limit )
        {
            // FIXME: The following algorithm is incorrect for cases in which
            //        the requested triple pattern contains a specific variable
            //        multiple times;
            //        e.g., (?x foaf:knows ?x ) or (_:bn foaf:knows _:bn)
            // see https://github.com/LinkedDataFragments/Server.Java/issues/23

            // look up the result from the HDT datasource)

           // System.out.println("look up the result from the HDT datasource)");
            int subjectId = subject.isVariable() ? 0 : dictionary.getIntID(subject.asConstantTerm().asNode(), TripleComponentRole.SUBJECT);
            int predicateId = predicate.isVariable() ? 0 : dictionary.getIntID(predicate.asConstantTerm().asNode(), TripleComponentRole.PREDICATE);
            int objectId = object.isVariable() ? 0 : dictionary.getIntID(object.asConstantTerm().asNode(), TripleComponentRole.OBJECT);

            if (subjectId < 0 || predicateId < 0 || objectId < 0) {
                return createEmptyTriplePatternFragment();
            }

            final Model triples = ModelFactory.createDefaultModel();
            IteratorTripleID matches = datasource.getTriples().search(new TripleID(subjectId, predicateId, objectId));
            boolean hasMatches = matches.hasNext();
            int found = 0;

            if (hasMatches) {
                // try to jump directly to the offset
                boolean atOffset;
                if (matches.canGoTo()) {
                    try {
                        matches.goTo(offset);
                        atOffset = true;
                    } // if the offset is outside the bounds, this page has no matches
                    catch (IndexOutOfBoundsException exception) {
                        atOffset = false;
                    }
                } // if not possible, advance to the offset iteratively
                else {
                    matches.goToStart();
                    for (int i = 0; !(atOffset = i == offset) && matches.hasNext(); i++) {
                        matches.next();
                    }
                }
                // try to add `limit` triples to the result model
                if (atOffset) {
                    for (int i = 0; i < limit && matches.hasNext(); i++) {
                        triples.add(triples.asStatement(toTriple(matches.next())));
                        found++;
                    }
                }
            }

            // estimates can be wrong; ensure 0 is returned if there are no results,
            // and always more than actual results
            final long estimatedTotal = triples.size() > 0 ?
                    Math.max(offset + triples.size() + 1, matches.estimatedNumResults())
                    : hasMatches ?
                            Math.max(matches.estimatedNumResults(), 1)
                            : 0;

            // create the fragment
            final boolean isLastPage = ( estimatedTotal < offset + limit );
            return createTriplePatternFragment( triples, estimatedTotal, isLastPage, found );
        }

    } // end of Worker

    /**
     * Converts the HDT triple to a Jena Triple.
     *
     * @param tripleId the HDT triple
     * @return the Jena triple
     */
    private Triple toTriple(TripleID tripleId) {
        return new Triple(
            dictionary.getNode((int)tripleId.getSubject(), TripleComponentRole.SUBJECT),
            dictionary.getNode((int)tripleId.getPredicate(), TripleComponentRole.PREDICATE),
            dictionary.getNode((int)tripleId.getObject(), TripleComponentRole.OBJECT)
        );
    }

}