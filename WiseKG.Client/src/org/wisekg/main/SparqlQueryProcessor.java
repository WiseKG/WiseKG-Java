/**
 *
 */
package org.wisekg.main;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.wisekg.callable.WiseKGHttpRequestThread;
import org.wisekg.executionplan.QueryExecutionPlan;
import org.wisekg.task.WiseKGHttpRequestTask;
import org.apache.http.client.fluent.Content;
import org.wisekg.callable.InitialHttpRequestThread;
import org.wisekg.main.QueryInput.QueryProcessingMethod;
import org.wisekg.model.BindingHashMap;
import org.wisekg.model.TriplePattern;
import org.wisekg.task.InitialHttpRequestTask;
import org.rdfhdt.hdt.hdt.HDT;

public class SparqlQueryProcessor {
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ConcurrentHashMap<String, HDT> hdtResponseCache;
    public static Queue<String> cacheQueue;

    private ArrayList<TriplePattern> triplePatterns;
    private QueryExecutionPlan executionPlan = QueryExecutionPlan.getNullPlan();

    public static QueryProcessingMethod method = QueryProcessingMethod.SPF;
    private final int nThreads;
    private ExecutorService executorService;
    private ExecutorCompletionService<Boolean> executorCompletionService;

    private ConcurrentLinkedQueue<BindingHashMap> outputBindings;
    private AtomicInteger numberOfTasks;
    private QueryInput input;
    private long queryProcessingTime;
    public static AtomicInteger SERVER_REQUESTS = new AtomicInteger(0);
    public static AtomicLong TRANSFERRED_BYTES = new AtomicLong(0);
    public static boolean RECEIVED_RESULT = false;
    public static long RESPONSE_TIME = 0;
    public static long START_TIME = 0;
    public static final int MAX_CACHE_ENTRIES = 1000;

    public boolean printOutput;
    public static AtomicInteger NUMBER_OF_HTTP_REQUESTS = new AtomicInteger(0);
    public static AtomicInteger NUMBER_OF_BINDINGS_SENT = new AtomicInteger(0);
    public static AtomicInteger NUMBER_OF_BINDINGS_RECEIVED = new AtomicInteger(0);
    public static int NUMBER_OF_OUTPUT_BINDINGS = 0;
    public static List<Integer> FRAGMENT_SIZES = new ArrayList<>();


    public SparqlQueryProcessor(ArrayList<TriplePattern> triplePatterns,
                                QueryInput input, boolean isMultiThreaded, boolean printOutput) {
        this.triplePatterns = triplePatterns;

        this.input = input;
        httpResponseCache = new ConcurrentHashMap<String, Content>();
        hdtResponseCache = new ConcurrentHashMap<>();
        cacheQueue = new LinkedList<>();
        outputBindings = new ConcurrentLinkedQueue<BindingHashMap>();
        if (isMultiThreaded) {
            nThreads = Runtime.getRuntime().availableProcessors();
        } else {
            nThreads = 1;
        }
        this.printOutput = printOutput;
    }

    public SparqlQueryProcessor(ArrayList<TriplePattern> triplePatterns,
                                QueryInput input, boolean isMultiThreaded, boolean printOutput, boolean tpf) {
        this.triplePatterns = triplePatterns;

        this.input = input;
        httpResponseCache = new ConcurrentHashMap<String, Content>();
        hdtResponseCache = new ConcurrentHashMap<>();
        cacheQueue = new LinkedList<>();
        outputBindings = new ConcurrentLinkedQueue<BindingHashMap>();
        if (isMultiThreaded) {
            nThreads = Runtime.getRuntime().availableProcessors();
        } else {
            nThreads = 1;
        }
        this.printOutput = printOutput;

        if (tpf)
            method = QueryProcessingMethod.TPF;
    }

    private void getExecutionPlan() {
        executorService = Executors.newFixedThreadPool(nThreads);
        InitialHttpRequestTask httpRequestTask = new InitialHttpRequestTask(triplePatterns);
        Future<QueryExecutionPlan> f = executorService.submit(new InitialHttpRequestThread(httpRequestTask, httpResponseCache));
        try {
            executionPlan = f.get();
        } catch (InterruptedException | ExecutionException e) {
            executionPlan = QueryExecutionPlan.getNullPlan();
        }
        executorService.shutdown();
    }

    private void initializeProcessingQuery() {
        executorService = Executors.newFixedThreadPool(nThreads);
        executorCompletionService = new ExecutorCompletionService<Boolean>(executorService);
        WiseKGHttpRequestTask httpRequestTask = new WiseKGHttpRequestTask(new ArrayList<>(), outputBindings, executionPlan);
        numberOfTasks = new AtomicInteger(1);
        WiseKGHttpRequestThread hrt = new WiseKGHttpRequestThread(httpRequestTask, httpResponseCache, hdtResponseCache,
                executorCompletionService, numberOfTasks);
        executorCompletionService.submit(hrt);
    }

    public void processQuery() throws InterruptedException, ExecutionException {
        WiseKGHttpRequestThread.started = false;

        SERVER_REQUESTS = new AtomicInteger(0);
        TRANSFERRED_BYTES = new AtomicLong(0);
        RECEIVED_RESULT = false;
        RESPONSE_TIME = 0;
        NUMBER_OF_OUTPUT_BINDINGS = 0;
        NUMBER_OF_BINDINGS_RECEIVED.set(0);
        FRAGMENT_SIZES.clear();
        FRAGMENT_SIZES = new ArrayList<>();

        START_TIME = System.currentTimeMillis();
        long start = System.currentTimeMillis();

        getExecutionPlan();

        if (!executionPlan.isNullPlan()) {
            initializeProcessingQuery();
            while (numberOfTasks.get() != 0) {
                executorCompletionService.take();
                numberOfTasks.decrementAndGet();
            }
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }
        if (printOutput) {
            System.out.println(outputBindings.size());
        }
        NUMBER_OF_OUTPUT_BINDINGS = outputBindings.size();

        long end = System.currentTimeMillis();
        queryProcessingTime = end - start;
    }

    public void printBindings() {
        Iterator<BindingHashMap> outputBindingsIterator = outputBindings.iterator();
        while (outputBindingsIterator.hasNext()) {
            BindingHashMap currentBinding = outputBindingsIterator.next();
            System.out.println(currentBinding.toString());
        }
        System.err.println("Number of output bindings: " + outputBindings.size());
        System.err.println("Runtime: " + Duration.ofMillis(queryProcessingTime));
    }

    public void terminate() {
        if (executorService != null) executorService.shutdownNow();
    }

    public void close() {
        for (Map.Entry<String, HDT> en : hdtResponseCache.entrySet()) {
            try {
                en.getValue().close();
            } catch (IOException e) {
            }
        }

        hdtResponseCache.clear();
        httpResponseCache.clear();
        outputBindings.clear();
        cacheQueue.clear();
    }

    public long getQueryProcessingTime() {
        return queryProcessingTime;
    }

    public ConcurrentLinkedQueue<BindingHashMap> getOutputBindings() {
        return outputBindings;
    }
}
