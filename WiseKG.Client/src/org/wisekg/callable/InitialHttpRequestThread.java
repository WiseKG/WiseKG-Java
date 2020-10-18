package org.wisekg.callable;

import java.io.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.wisekg.executionplan.QueryExecutionPlan;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.wisekg.main.SparqlQueryProcessor;
import org.wisekg.task.InitialHttpRequestTask;
import org.wisekg.util.PlanDeserializer;

public class InitialHttpRequestThread implements Callable<QueryExecutionPlan> {
    private InitialHttpRequestTask httpRequestTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;

    public InitialHttpRequestThread(InitialHttpRequestTask httpRequestTask, ConcurrentHashMap<String, Content> httpResponseCache) {
        this.httpRequestTask = httpRequestTask;
        this.httpResponseCache = httpResponseCache;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public QueryExecutionPlan call() throws Exception {
        QueryExecutionPlan plan = QueryExecutionPlan.getNullPlan();
        try {
            String httpUrl = httpRequestTask.getFragmentURL();
            Content content = null;
            boolean cacheContains = false;
            if (httpResponseCache.containsKey(httpUrl)) {
                cacheContains = true;
                content = httpResponseCache.get(httpUrl);
            } else {
                SparqlQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();

                SparqlQueryProcessor.SERVER_REQUESTS.incrementAndGet();
                content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
                SparqlQueryProcessor.TRANSFERRED_BYTES.addAndGet(content.asBytes().length + httpUrl.getBytes().length);
            }

            GsonBuilder gs = new GsonBuilder();
            gs.registerTypeAdapter(QueryExecutionPlan.class, new PlanDeserializer());
            Gson gson = gs.create();
            plan = gson.fromJson(content.asString(), QueryExecutionPlan.class);

            if (!cacheContains) {
                if(httpResponseCache.size() == SparqlQueryProcessor.MAX_CACHE_ENTRIES) {
                    httpResponseCache.remove(SparqlQueryProcessor.cacheQueue.poll());
                }
                httpResponseCache.put(httpUrl, content);
                SparqlQueryProcessor.cacheQueue.add(httpUrl);
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("There was an exception");
        }
        return plan;
    }
}
