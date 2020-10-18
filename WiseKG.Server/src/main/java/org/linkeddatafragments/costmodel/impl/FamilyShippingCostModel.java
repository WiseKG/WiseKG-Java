package org.linkeddatafragments.costmodel.impl;

import org.linkeddatafragments.costmodel.ICostModel;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.executionplan.QueryExecutionPlan;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.servlet.PartitioningServlet;
import org.linkeddatafragments.util.StarString;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class FamilyShippingCostModel implements ICostModel {
    private final ConfigReader config;
    private long cardinality = 0;
    private long prevCardinality = 0;
    private double elementSize = 0;
    private double cpuThreshold = 0.8;
    private final StarString star;
    private final QueryExecutionPlan plan;
    private String fileSource = "";

    public FamilyShippingCostModel(ConfigReader config, StarString star, QueryExecutionPlan plan) {
        this.config = config;
        this.star = star;
        this.plan = plan;
    }

    @Override
    public double cost(HttpServletRequest request, IDataSource datasource) {
        this.fileSource = datasource.getFilename();
        if(cardinality == 0)
            calculateCardinality(request, datasource);
        //System.out.println("SmartKG Cost: " + dataTransferCost(request) + " " + communicationCost(request));
        //return dataTransferCost(request) + communicationCost(request) + processingCost(request) + ioCost(request);
        double cpu = getCPULoad();
        if(cpu >= cpuThreshold)
            return 0.0;
        return dataTransferCost(request) + communicationCost(request);
        //return dataTransferCost(request);
    }

    private void calculateCardinality(HttpServletRequest request, IDataSource datasource) {
        try {
            cardinality = datasource.cardinalityEstimation(star, plan);
            prevCardinality = plan.cardinalityEstimation(datasource);
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
        long bytes = 0;
        File file = new File(fileSource);
        bytes += file.length();

        String indexFilename = fileSource + ".index.v1-1";
        File indexFile = new File(indexFilename);

        if(indexFile.exists())
            bytes += indexFile.length();

        double speed = Double.parseDouble(request.getParameter("speed")) * 1000000;

        return (double) bytes / speed;
    }

    private double communicationCost(HttpServletRequest request) {
        return ((double)numberOfRequests() * Double.parseDouble(request.getParameter("latency"))) / 1000.0;
    }

    private int numberOfRequests() {
        return 2;
    }

    private double processingCost(HttpServletRequest request) {
        return (double)cardinality / Double.parseDouble(request.getParameter("ips"));
    }

    private double getCPULoad() {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        CentralProcessor cpu = hal.getProcessor();
        return cpu.getSystemLoadAverage(1)[0] / (double) cpu.getLogicalProcessorCount();
        //return 0.5;
    }

    private double ioCost(HttpServletRequest request) {
        return 0;
    }
}
