package org.wisekg.task;

import org.wisekg.callable.WiseKGHttpRequestThread;
import org.wisekg.executionplan.QueryExecutionPlan;
import org.wisekg.main.SparqlQueryProcessor;
import org.wisekg.model.BindingHashMap;
import org.wisekg.model.HttpRequestConfig;
import org.wisekg.util.QueryProcessingUtils;
import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.rdfhdt.hdt.hdt.HDT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;

public class StarHandlerSingleHdt extends AbstractRDFHandler {
    private List<List<Statement>> stars;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private HdtParseResponseTask parseResponseTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ConcurrentHashMap<String, HDT> hdtResponseCache;
    private AtomicInteger numberOfTasks;
    private HashSet<Statement> processedTriples;
    private static final int HYDRA_NEXTPAGE_HASH =
            new String("http://www.w3.org/ns/hydra/core#next").hashCode();
    private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
    private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();

    /**
     *
     */
    public StarHandlerSingleHdt(ExecutorCompletionService<Boolean> executorCompletionService,
                             HdtParseResponseTask parseResponseTask,
                             ConcurrentHashMap<String, Content> httpResponseCache, ConcurrentHashMap<String, HDT> hdtResponseCache,
                             AtomicInteger numberOfTasks) {
        this.stars = new ArrayList<>();
        this.executorCompletionService = executorCompletionService;
        this.parseResponseTask = parseResponseTask;
        this.httpResponseCache = httpResponseCache;
        this.hdtResponseCache = hdtResponseCache;
        this.numberOfTasks = numberOfTasks;
        this.processedTriples = new HashSet<>();
    }

    private boolean isTripleValid(Statement st) {
        if (st.getSubject().stringValue()
                .equals(parseResponseTask.getHttpRequestTask().getFragmentURL())) {
            if (st.getPredicate().stringValue().hashCode() == HYDRA_NEXTPAGE_HASH) {
                String fragmentURL = st.getObject().stringValue();
                WiseKGHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
                WiseKGHttpRequestTask httpRequestTask = new WiseKGHttpRequestTask(currHttpRequestTask.getBindings(),
                        currHttpRequestTask.getOutputBindings(), currHttpRequestTask.getPlan(), fragmentURL);
                numberOfTasks.incrementAndGet();
                WiseKGHttpRequestThread hrt = new WiseKGHttpRequestThread(httpRequestTask, httpResponseCache, hdtResponseCache,
                        executorCompletionService, numberOfTasks);
                executorCompletionService.submit(hrt);
            }
            return false;
        } else if (st.getPredicate().stringValue().contains("hydra/")
                || st.getObject().stringValue().contains("hydra/")
                || st.getObject().stringValue().hashCode() == DATASET_HASH
                || st.getPredicate().stringValue().hashCode() == SUBSET_HASH) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        if (stars.size() != 0) {
            SparqlQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(stars.size());
            sendRequestWithExtendedBindings();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.openrdf.rio.helpers.AbstractRDFHandler#handleStatement(org.openrdf.model.Statement)
     */
    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        if (processedTriples.contains(st)) {
            return;
        } else {
            processedTriples.add(st);
        }
        if (isTripleValid(st)) {
            List<Statement> star = new ArrayList<>();
            star.add(st);
            stars.add(star);
            if (stars.size() == HttpRequestConfig.MAX_NUMBER_OF_BINDINGS) {
                SparqlQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(stars.size());
                sendRequestWithExtendedBindings();
                stars.clear();
            }
        }
    }

    private void sendRequestWithExtendedBindings() {
        WiseKGHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
        ArrayList<BindingHashMap> extendedBindings = QueryProcessingUtils.extendBindings(
                currHttpRequestTask.getBindings(), currHttpRequestTask.getStarPattern(), stars);
        ConcurrentLinkedQueue<BindingHashMap> outputBindings = currHttpRequestTask.getOutputBindings();
        QueryExecutionPlan subplan = currHttpRequestTask.getSubplan();
        if (subplan.isNullPlan()) {
            if(!SparqlQueryProcessor.RECEIVED_RESULT) {
                SparqlQueryProcessor.RECEIVED_RESULT = true;
                SparqlQueryProcessor.RESPONSE_TIME = System.currentTimeMillis() - SparqlQueryProcessor.START_TIME;
            }

            outputBindings.addAll(extendedBindings);
        } else {
            int size = extendedBindings.size();
            for(int i = 0; i < size; i += HttpRequestConfig.MAX_NUMBER_OF_BINDINGS) {
                int endIndex = i + HttpRequestConfig.MAX_NUMBER_OF_BINDINGS;
                if(endIndex > size) endIndex = size;
                ArrayList<BindingHashMap> sublist = new ArrayList<>(extendedBindings.subList(i, endIndex));

                WiseKGHttpRequestTask httpRequestTask = new WiseKGHttpRequestTask(sublist, outputBindings, subplan);
                numberOfTasks.incrementAndGet();
                WiseKGHttpRequestThread hrt = new WiseKGHttpRequestThread(httpRequestTask, httpResponseCache, hdtResponseCache,
                        executorCompletionService, numberOfTasks);
                executorCompletionService.submit(hrt);
            }
        }
    }
}
