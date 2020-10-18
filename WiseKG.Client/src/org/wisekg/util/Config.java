package org.wisekg.util;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.wisekg.main.Experiment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Config {

    private static Config INSTANCE;
    private String datasource;
    private boolean cacheReuse;
    private String server;
    private String metadatapath;
    private String downloadedpartitions;
    private String queriespath;
    private String ExperimentName;
    private String planner;
    private int TimeInMin;
    private Map<String, String> prefixes;
    private String characteristicsets;
    private int speed = 20;
    private double latency = 0.144;
    private double ips = 15139.47;


    private Config() {

    }

    public Config(String configFileName) {
        JsonReader configReader = null;
        if (INSTANCE == null) {
            File configFile;

            prefixes = Maps.newHashMap();
            configFile = new File(configFileName);
            try {
                configReader = new JsonReader(new FileReader(configFile));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Config.class.getName()).log(Level.SEVERE, null, ex);
            }

            INSTANCE = new Gson().fromJson(configReader, Config.class);
            //INSTANCE.cs = loadCSs(INSTANCE.characteristicsets);
        }
    }

    public int getSpeed() {
        return speed;
    }

    public double getIps() {
        return ips;
    }

    private static String readFile(String filePath) {
        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }


    public double getLatency() {
        return latency;
    }

    public static Config getInstance() {
        return INSTANCE;
    }


    public String getExperimentName() {
        return ExperimentName;
    }

    public String getPlanner() {
        return planner;
    }

    /**
     * @return the datasource
     */
    public String getDatasource() {
        return datasource;
    }

    public Map<String, String> getPrefixes() {
        return prefixes;
    }

    /**
     * @return the server
     */
    public String getServer() {
        return server;
    }

    /**
     * @return the metadatapath
     */
    public String getMetadatapath() {
        return metadatapath;
    }

    /**
     * @return the queriespath
     */
    public String getQueriespath() {
        return queriespath;
    }

    /**
     * @return the downloadedpartitions
     */
    public String getDownloadedpartitions() {
        String path = downloadedpartitions + "client"+ Experiment.CLIENT_NUM+"/";
        new File(path).mkdirs();
        return path;
    }

    /**
     * @return the cacheEnabled
     */
    public boolean isCacheResued() {
        return cacheReuse;
    }

    /**
     * @return the TimeInMin
     */
    public int getTimeInMin() {
        return TimeInMin;
    }

}
