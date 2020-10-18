package org.linkeddatafragments.costmodel.impl;

import org.linkeddatafragments.costmodel.ICostModel;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.servlet.PartitioningServlet;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import javax.servlet.http.HttpServletRequest;

public class HeuristicCostModel implements ICostModel {
    private final double cpuThreshold;
    private final long cardinalityThreshold;
    private final ConfigReader config;

    public HeuristicCostModel(double cpuThreshold, long cardinalityThreshold, ConfigReader config) {
        this.cpuThreshold = cpuThreshold;
        this.cardinalityThreshold = cardinalityThreshold;
        this.config = config;
    }

    @Override
    public double cost(HttpServletRequest request, IDataSource datasource) {
        double load = getCPULoad();
        if (load <= cpuThreshold)
            return 0;
        return 1;
    }

    private double getCPULoad() {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        CentralProcessor cpu = hal.getProcessor();
        return cpu.getSystemLoadAverage(1)[0] / (double) cpu.getLogicalProcessorCount();
    }
}
