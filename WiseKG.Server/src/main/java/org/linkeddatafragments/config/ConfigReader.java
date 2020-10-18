package org.linkeddatafragments.config;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.linkeddatafragments.characteristicset.CharacteristicSetImpl;
import org.linkeddatafragments.characteristicset.ICharacteristicSet;
import org.linkeddatafragments.datasource.IDataSourceType;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.linkeddatafragments.queryAnalyzer.Config;


public class ConfigReader {
    private final Map<String, IDataSourceType> dataSourceTypes = new HashMap<>();
    private final Map<String, JsonObject> dataSources = new HashMap<>();
    private final Map<String, String> prefixes = new HashMap<>();
    private String baseURL;
    private String metadatapath;
    private String moleculesdatapath;
    private String partitionstring;
    private String defaultGraph;
    private String uri;
    private List<ICharacteristicSet> characteristicSets;
    private static ConfigReader instance = new ConfigReader();

    public ConfigReader() {}
    /**
     * Creates a new configuration reader.
     *
     * @param configReader the configuration
     */
    public ConfigReader(Reader configReader) {
        JsonObject root = new JsonParser().parse(configReader).getAsJsonObject();
        this.baseURL = root.has("baseURL") ? root.getAsJsonPrimitive("baseURL").getAsString() : null;
        this.metadatapath = root.has("metadatapath") ? root.getAsJsonPrimitive("metadatapath").getAsString() : null;
        this.moleculesdatapath = root.has("moleculesdatapath") ? root.getAsJsonPrimitive("moleculesdatapath").getAsString() : null;
        this.partitionstring = root.has("partstring") ? root.getAsJsonPrimitive("partstring").getAsString() : null;
        this.defaultGraph = root.has("default") ? root.getAsJsonPrimitive("default").getAsString() : null;
        this.uri = root.has("uri") ? root.getAsJsonPrimitive("uri").getAsString() : null;
        for (Entry<String, JsonElement> entry : root.getAsJsonObject("datasourcetypes").entrySet()) {
            final String className = entry.getValue().getAsString();
            dataSourceTypes.put(entry.getKey(), initDataSouceType(className) );
        }
        for (Entry<String, JsonElement> entry : root.getAsJsonObject("datasources").entrySet()) {
            JsonObject dataSource = entry.getValue().getAsJsonObject();
            this.dataSources.put(entry.getKey(), dataSource);
        }
        for (Entry<String, JsonElement> entry : root.getAsJsonObject("prefixes").entrySet()) {
            this.prefixes.put(entry.getKey(), entry.getValue().getAsString());
        }
        String cpath = root.has("cspath") ? root.getAsJsonPrimitive("cspath").getAsString() : null;
        if(cpath != null) {
            Gson gson = new Gson();
            File f = new File(cpath);
            try {
                FileReader fr = new FileReader(f);
                BufferedReader br = new BufferedReader(fr);

                String line;
                while ((line = br.readLine()) != null) {
                    if (line.equals("")) continue;
                    Type collectionType = new TypeToken<CharacteristicSetImpl>() {
                    }.getType();
                    this.characteristicSets.add(gson.fromJson(line, collectionType));
                }
            } catch (Exception e) {
                this.characteristicSets = null;
            }
        } else {
            this.characteristicSets = null;
        }
        instance = this;
    }

    public String getUri() {
        return uri;
    }

    public static ConfigReader getInstance() {
        return instance;
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

    public String getPartitionstring() {
        return partitionstring;
    }

    public String getDefaultGraph() {
        return defaultGraph;
    }

    /**
     * Gets the data source types.
     *
     * @return a mapping of names of data source types to these types
     */
    public Map<String, IDataSourceType> getDataSourceTypes() {
        return dataSourceTypes;
    }

    /**
     * Gets the data sources.
     *
     * @return the data sources
     */
    public Map<String, JsonObject> getDataSources() {
        return dataSources;
    }

    public List<ICharacteristicSet> getCharacteristicSets() {
        return characteristicSets;
    }

    /**
     * Gets the prefixes.
     *
     * @return the prefixes
     */
    public Map<String, String> getPrefixes() {
        return prefixes;
    }

    /**
     * Gets the base URL
     *
     * @return the base URL
     */
    public String getBaseURL() {
        return baseURL;
    }

      /**
     * @return the metadatapath
     */
    public String getMetadatapath() {
        return metadatapath;
    }

    /**
     * @return the moleculesdatapath
     */
    public String getMoleculesdatapath() {
        return moleculesdatapath;
    }

    /**
     * Loads a certain {@link IDataSourceType} class at runtime
     *
     * @param className IDataSourceType class
     * @return the created IDataSourceType object
     */
    protected IDataSourceType initDataSouceType( final String className )
    {
        final Class<?> c;
        try {
            c = Class.forName( className );
        }
        catch ( ClassNotFoundException e ) {
            throw new IllegalArgumentException( "Class not found: " + className,
                                                e );
        }

        final Object o;
        try {
            o = c.newInstance();
        }
        catch ( Exception e ) {
            throw new IllegalArgumentException(
                        "Creating an instance of class '" + className + "' " +
                        "caused a " + e.getClass().getSimpleName() + ": " +
                        e.getMessage(), e );
        }

        if ( ! (o instanceof IDataSourceType) )
            throw new IllegalArgumentException(
                        "Class '" + className + "' is not an implementation " +
                        "of IDataSourceType." );

        return (IDataSourceType) o;
    }



}
