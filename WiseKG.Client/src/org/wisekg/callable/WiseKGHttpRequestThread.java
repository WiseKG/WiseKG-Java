package org.wisekg.callable;

import org.wisekg.main.Experiment;
import org.wisekg.main.SparqlQueryProcessor;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.wisekg.task.HdtParseResponseTask;
import org.wisekg.task.InitialHttpRequestTask;
import org.wisekg.task.WiseKGHttpRequestTask;
import org.wisekg.task.WiseKGParseResponseTask;
import org.wisekg.util.Config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.IllegalFormatException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WiseKGHttpRequestThread implements Callable<Boolean> {
    private WiseKGHttpRequestTask httpRequestTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ConcurrentHashMap<String, HDT> hdtResponseCache;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private AtomicInteger numberOfTasks;

    public static int currtpIdx = 0;
    public static boolean started = false;
    public static long startTime = 0;
    public static String currResStr = "";
    public static long totalBindings = 0;
    public static String currMethod = "";

    public WiseKGHttpRequestThread(WiseKGHttpRequestTask httpRequestTask,
                                   ConcurrentHashMap<String, Content> httpResponseCache,
                                   ConcurrentHashMap<String, HDT> hdtResponseCache,
                                   ExecutorCompletionService<Boolean> executorCompletionService, AtomicInteger numberOfTasks) {
        this.httpRequestTask = httpRequestTask;
        this.httpResponseCache = httpResponseCache;
        this.hdtResponseCache = hdtResponseCache;
        this.executorCompletionService = executorCompletionService;
        this.numberOfTasks = numberOfTasks;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Boolean call() throws IOException {
        if(httpRequestTask.getPlan().getTimestamp() < System.currentTimeMillis()) {
            InitialHttpRequestTask task = new InitialHttpRequestTask(httpRequestTask.getPlan().getTriples());
            InitialHttpRequestThread thread = new InitialHttpRequestThread(task, httpResponseCache);
            try {
                httpRequestTask.setPlan(thread.call());
            } catch (Exception e) {}
        }

        String control = httpRequestTask.getOperator().getControl();
        String filePathString = Config.getInstance().getDownloadedpartitions() + control.substring(control.lastIndexOf("/")+1);
        if (hdtResponseCache.containsKey(filePathString)) {
            //currMethod = "SMARTKG";
            HdtParseResponseTask prTask = new HdtParseResponseTask(httpRequestTask);
            numberOfTasks.incrementAndGet();
            HdtResponseParserThread rpThread = new HdtResponseParserThread(prTask, executorCompletionService, httpResponseCache,
                    hdtResponseCache, numberOfTasks, httpRequestTask.getStarPattern(), hdtResponseCache.get(filePathString));
            executorCompletionService.submit(rpThread);
        } else {
            String httpUrl = httpRequestTask.getFragmentURL();

            Content content = null;
            if (httpResponseCache.containsKey(httpUrl)) {
                content = httpResponseCache.get(httpUrl);
            } else {
                SparqlQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
                SparqlQueryProcessor.NUMBER_OF_BINDINGS_SENT.addAndGet(httpRequestTask.getBindings().size());

                SparqlQueryProcessor.SERVER_REQUESTS.incrementAndGet();
                content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
                SparqlQueryProcessor.TRANSFERRED_BYTES.addAndGet(content.asBytes().length + httpUrl.getBytes().length);

                httpResponseCache.put(httpUrl, content);
            }
            if (content != null) {
                if (content.asString().startsWith("$HDT")) {
                    //currMethod = "SMARTKG";
                    InputStream stream = content.asStream();
                    saveHdtFile(filePathString, stream);

                    String httpUrlIndex = httpRequestTask.getFragmentURL().replace(".hdt", ".hdt.index.v1-1");
                    Content contentIndex = Request.Get(httpUrlIndex).addHeader("accept", "text/turtle").execute().returnContent();
                    InputStream inputStreamIndex = contentIndex.asStream();
                    String filePathStringIndex = filePathString.replace(".hdt", ".hdt.index.v1-1");
                    saveHdtFile(filePathStringIndex, inputStreamIndex);

                    try {
                        hdtResponseCache.put(filePathString, HDTManager.mapIndexedHDT(filePathString));
                    } catch (IllegalFormatException e) {
                        throw new HdtException("Error for client " + Experiment.CLIENT_NUM + " query " + Experiment.QUERY);
                    }

                    HdtParseResponseTask prTask = new HdtParseResponseTask(httpRequestTask);
                    numberOfTasks.incrementAndGet();
                    HdtResponseParserThread rpThread = new HdtResponseParserThread(prTask, executorCompletionService, httpResponseCache,
                            hdtResponseCache, numberOfTasks, httpRequestTask.getStarPattern(), hdtResponseCache.get(filePathString));
                    executorCompletionService.submit(rpThread);
                } else {
                    WiseKGParseResponseTask prTask = new WiseKGParseResponseTask(httpRequestTask, content.asStream());
                    numberOfTasks.incrementAndGet();
                    WiseKGResponseParserThread rpThread = new WiseKGResponseParserThread(prTask, executorCompletionService,
                            httpResponseCache, hdtResponseCache, numberOfTasks);
                    executorCompletionService.submit(rpThread);
                }
            }
        }
        return true;
    }

    private class HdtException extends RuntimeException {
        public HdtException(String msg) {
            super(msg);
        }
    }

    private void saveHdtFile(String path, InputStream stream) throws IOException {
        Files.copy(stream, Paths.get(".", path), StandardCopyOption.REPLACE_EXISTING);
        stream.close();
    }
}
