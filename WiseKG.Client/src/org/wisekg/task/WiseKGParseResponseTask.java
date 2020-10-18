package org.wisekg.task;

import java.io.InputStream;

public class WiseKGParseResponseTask {
    private WiseKGHttpRequestTask httpRequestTask;
    private InputStream responseStream;

    public WiseKGParseResponseTask(WiseKGHttpRequestTask httpRequestTask, InputStream responseStream) {
        this.httpRequestTask = httpRequestTask;
        this.responseStream = responseStream;
    }


    public WiseKGHttpRequestTask getHttpRequestTask() {
        return httpRequestTask;
    }


    public InputStream getResponseStream() {
        return responseStream;
    }
}
