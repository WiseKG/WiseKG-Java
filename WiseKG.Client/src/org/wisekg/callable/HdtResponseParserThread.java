package org.wisekg.callable;

import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.util.StarString;
import org.wisekg.task.HdtParseResponseTask;
import org.wisekg.task.StarHandlerHdt;
import org.wisekg.task.StarHandlerSingleHdt;
import org.wisekg.task.WiseKGHttpRequestTask;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;

public class HdtResponseParserThread implements Callable<Boolean> {
    private HdtParseResponseTask parseResponseTask;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ConcurrentHashMap<String, HDT> hdtResponseCache;
    private AtomicInteger numberOfTasks;
    private StarString star;
    private HDT hdt;

    public HdtResponseParserThread(HdtParseResponseTask parseResponseTask,
                                   ExecutorCompletionService<Boolean> executorCompletionService,
                                   ConcurrentHashMap<String, Content> httpResponseCache, ConcurrentHashMap<String, HDT> hdtResponseCache,
                                   AtomicInteger numberOfTasks, StarString star, HDT hdt) {
        this.parseResponseTask = parseResponseTask;
        this.executorCompletionService = executorCompletionService;
        this.httpResponseCache = httpResponseCache;
        this.hdtResponseCache = hdtResponseCache;
        this.numberOfTasks = numberOfTasks;
        this.star = star;
        this.hdt = hdt;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Boolean call() throws RDFParseException, RDFHandlerException, IOException {
        WiseKGHttpRequestTask task = parseResponseTask.getHttpRequestTask();
        if(task.isSingle()) {
            StarHandlerSingleHdt starHandler = new StarHandlerSingleHdt(executorCompletionService, parseResponseTask,
                    httpResponseCache, hdtResponseCache, numberOfTasks);
            HdtProcessorStar processor = new HdtProcessorStar(starHandler, star, hdt, parseResponseTask.getHttpRequestTask().getBindings());
            processor.process();
        } else {

            StarHandlerHdt starHandler = new StarHandlerHdt(executorCompletionService, parseResponseTask,
                    httpResponseCache, hdtResponseCache, numberOfTasks);
            HdtProcessorStar processor = new HdtProcessorStar(starHandler, star, hdt, parseResponseTask.getHttpRequestTask().getBindings());
            processor.process();
        }
        return true;
    }
}
