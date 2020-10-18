package org.linkeddatafragments.costmodel.impl;

import org.linkeddatafragments.costmodel.ICostModel;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.executionplan.QueryExecutionPlan;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.StarString;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.http.HttpServletRequest;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class StarCostModel implements ICostModel {

    private long cardinality = 0;
    private long prevCardinality = 0;
    private double elementSize = 0;
    private int sockets = 32;
    private int cps = 1;
    private double clock = 1993.221;
    private double ipc = 5;
    private final StarString star;
    private final QueryExecutionPlan plan;

    public StarCostModel(StarString star, QueryExecutionPlan plan) {
        this.star = star;
        this.plan = plan;
    }

    @Override
    public double cost(HttpServletRequest request, IDataSource datasource) {
        if(cardinality == 0)
            calculateCardinality(request, datasource);

        //System.out.println("SPF Cost: " + dataTransferCost(request) + " " + communicationCost(request));

        //return dataTransferCost(request) + communicationCost(request) + processingCost(request);
        return dataTransferCost(request) + communicationCost(request);
        //return dataTransferCost(request);
    }

    private void calculateCardinality(HttpServletRequest request, IDataSource dataSource) {
        try {
            cardinality = dataSource.cardinalityEstimation(star, plan);
            prevCardinality = plan.cardinalityEstimation(dataSource);
            //elementSize = ldfProcessor.meanElementSize(ldfRequest);
            String str = "http://purl.org/goodrelations/includes";
            elementSize = str.getBytes().length;
        } catch (Exception e) {
            cardinality = 0;
        }

        if(request.getParameter("card") != null)
            cardinality = Long.parseLong(request.getParameter("card"));
    }

    private double dataTransferCost(HttpServletRequest request) {
        // Calculate the triple pattern element size

        int triples;
        if(star == null)
            triples = Integer.parseInt(request.getParameter("triples"));
        else
            triples = star.size();
        int elementsPerResult = 1+(triples*2);

        double numBytes = (elementsPerResult*cardinality)*elementSize;
        //int numRequests = numberOfRequests(request);
        double speed = Double.parseDouble(request.getParameter("speed")) * 1000000;

        return numBytes / speed;
    }

    private double communicationCost(HttpServletRequest request) {
        return ((double)numberOfRequests() * Double.parseDouble(request.getParameter("latency"))) / 1000.0;
    }

    private int numberOfRequests() {
        return Math.max(1,(int) Math.ceil(((double)cardinality / (double)ILinkedDataFragmentRequest.TRIPLESPERPAGE) + ((double) prevCardinality / (double) ILinkedDataFragmentRequest.TRIPLESPERREQUEST)));
    }

    private double processingCost(HttpServletRequest request) {
        double ips = (double)sockets * (double)cps * clock * ipc;
        return cardinality / (ips * (1-getCPULoad()));
    }

    private double ioCost(HttpServletRequest request) {
        return 0;
    }

    private double getCPULoad() {
        //SystemInfo si = new SystemInfo();
        //HardwareAbstractionLayer hal = si.getHardware();
        //CentralProcessor cpu = hal.getProcessor();
        //return cpu.getSystemLoadAverage(1)[0] / (double) cpu.getLogicalProcessorCount();
        return 0.5;
    }
}
