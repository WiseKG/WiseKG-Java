package org.wisekg.task;

public class HdtParseResponseTask {
    private WiseKGHttpRequestTask httpRequestTask;

    public HdtParseResponseTask(WiseKGHttpRequestTask httpRequestTask) {
        this.httpRequestTask = httpRequestTask;
    }

    public WiseKGHttpRequestTask getHttpRequestTask() {
        return httpRequestTask;
    }
}
