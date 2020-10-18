package org.wisekg.callable;

import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import org.rdfhdt.hdt.hdt.HDT;
import org.wisekg.task.StarHandler;
import org.wisekg.task.StarHandlerSingle;
import org.wisekg.task.WiseKGHttpRequestTask;
import org.wisekg.task.WiseKGParseResponseTask;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;

public class WiseKGResponseParserThread implements Callable<Boolean> {

    private WiseKGParseResponseTask parseResponseTask;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ConcurrentHashMap<String, HDT> hdtResponseCache;
    private AtomicInteger numberOfTasks;

    public WiseKGResponseParserThread(WiseKGParseResponseTask parseResponseTask,
                                      ExecutorCompletionService<Boolean> executorCompletionService,
                                      ConcurrentHashMap<String, Content> httpResponseCache, ConcurrentHashMap<String, HDT> hdtResponseCache,
                                      AtomicInteger numberOfTasks) {
        this.parseResponseTask = parseResponseTask;
        this.executorCompletionService = executorCompletionService;
        this.httpResponseCache = httpResponseCache;
        this.hdtResponseCache = hdtResponseCache;
        this.numberOfTasks = numberOfTasks;
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
            TurtleParser turtleParser = new TurtleParser();
            turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
            turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
            turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

            StarHandlerSingle starHandler = new StarHandlerSingle(executorCompletionService, parseResponseTask,
                    httpResponseCache, hdtResponseCache, numberOfTasks);
            turtleParser.setRDFHandler(starHandler);
            turtleParser.parse(parseResponseTask.getResponseStream(), "");
        } else {
            TurtleParser turtleParser = new TurtleParserStar();
            turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
            turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
            turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

            StarHandler starHandler = new StarHandler(executorCompletionService, parseResponseTask,
                    httpResponseCache, hdtResponseCache, numberOfTasks);
            turtleParser.setRDFHandler(starHandler);
            turtleParser.parse(parseResponseTask.getResponseStream(), "");
        }
        return true;
    }
}
