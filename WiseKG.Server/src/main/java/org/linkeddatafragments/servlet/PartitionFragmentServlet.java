package org.linkeddatafragments.servlet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.http.HttpHeaders;
import org.apache.jena.riot.Lang;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.index.IndexDataSource;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.MIMEParse;
import org.linkeddatafragments.views.ILinkedDataFragmentWriter;
import org.linkeddatafragments.views.LinkedDataFragmentWriterFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import static org.linkeddatafragments.servlet.LinkedDataFragmentServlet.CFGFILE;

public class PartitionFragmentServlet extends HttpServlet {
    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    public static ConfigReader config;
    private static final int NO_DATASOURCES = 512;
    public static final HashMap<String, IDataSource> dataSources = new HashMap<>(NO_DATASOURCES);
    private final Collection<String> mimeTypes = new ArrayList<>();
    private static final double CPU_THRESHOLD = 0.6;
    private static final long CARDINALITY_THRESHOLD = 1000L;
    private FileWriter writer = null;

    public static double CPU_LOAD = 0;


    private File getConfigFile(ServletConfig config) throws IOException {
        String path = config.getServletContext().getRealPath("/");

        if (path == null) {
            // this can happen when running standalone
            path = System.getProperty("user.dir");
        }
        File cfg = new File("config-example.json");
        if (config.getInitParameter(CFGFILE) != null) {
            cfg = new File(config.getInitParameter(CFGFILE));
        }
        if (!cfg.exists()) {
            throw new IOException("Configuration file " + cfg + " not found.");
        }
        if (!cfg.isFile()) {
            throw new IOException("Configuration file " + cfg + " is not a file.");
        }
        return cfg;
    }

    /**
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        try {
            System.out.println("Partitions loading Configuration File");
            // load the configuration
            File configFile = getConfigFile(servletConfig);
            System.out.println("configFile:" + configFile.getName());
            config = new ConfigReader(new FileReader(configFile));

            System.out.println("Partitions ConfigReader =>" + config.getMoleculesdatapath());

            // register data source types
            //   for (Map.Entry<String, IDataSourceType> typeEntry : config.getDataSourceTypes().entrySet()) {
            //     DataSourceTypesRegistry.register(typeEntry.getKey(),
            //           typeEntry.getValue());
            //}

            // register data sources
            //String path = config.getMoleculesdatapath() + "/hdt";
            //File dir = new File(path);
            //File[] files = dir.listFiles();

            //for(File f : files) {
            //    if(f.getName().endsWith(".hdt")) {
            //        dataSources.put(f.getName(), DataSourceFactory.createMoleculeDatasource(f.getAbsolutePath()));
            //    }
            //}


            //for (Map.Entry<String, JsonObject> dataSource : config.getDataSources().entrySet()) {
            //   dataSources.put(dataSource.getKey(), DataSourceFactory.create(dataSource.getValue()));
            // }
            // register content types
            //MIMEParse.register("text/html");
            //MIMEParse.register(Lang.RDFXML.getHeaderString());
            //MIMEParse.register(Lang.NTRIPLES.getHeaderString());
            //MIMEParse.register(Lang.JSONLD.getHeaderString());
            MIMEParse.register(Lang.TTL.getHeaderString());
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    /**
     * metadatapath
     */
    @Override
    public void destroy() {
        for (IDataSource dataSource : dataSources.values()) {
            try {
                dataSource.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private IDataSource getDataSource(HttpServletRequest request) throws DataSourceNotFoundException {
        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

        //  System.out.println("contextPath =======>" + contextPath);
        //  System.out.println("requestURI =======>" + requestURI);
        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());

        if (path.equals("/") || path.isEmpty()) {
            final String baseURL = FragmentRequestParserBase.extractBaseURL(request, config);
            //  System.out.println("baseURL =======>" + baseURL);
            return new IndexDataSource(baseURL, dataSources);
        }

        String dataSourceName = path.substring(1).replace("partition/", "");
        return PartitioningServlet.getDataSource(dataSourceName);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        ILinkedDataFragment fragment = null;

        try {
            IDataSource dataSource = null;
            dataSource = getDataSource(request);

            String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
            String bestMatch = MIMEParse.bestMatch(acceptHeader);

            response.setHeader(HttpHeaders.SERVER, "Linked Data Fragments Server");
            response.setContentType(bestMatch);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());

            ILinkedDataFragmentWriter writer = LinkedDataFragmentWriterFactory.create(config.getPrefixes(), dataSources, bestMatch);

            try {
                ILinkedDataFragmentRequest ldfRequest = null;
                if (request.getParameter("triples") == null) {
                    if (request.getParameter("values") == null) {
                        ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.TPF)
                                .parseIntoFragmentRequest(request, config);
                        fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.TPF)
                                .createRequestedFragment(ldfRequest);
                    } else {
                        ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.BRTPF)
                                .parseIntoFragmentRequest(request, config);
                        fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.BRTPF)
                                .createRequestedFragment(ldfRequest);
                    }
                } else {
                    ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.SPF)
                            .parseIntoFragmentRequest(request, config);
                    fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.SPF)
                            .createRequestedFragment(ldfRequest);
                }

                writer.writeFragment(response.getOutputStream(), dataSource, fragment, ldfRequest);
            } catch (DataSourceNotFoundException ex) {
                try {
                    response.setStatus(404);
                    writer.writeNotFound(response.getOutputStream(), request);
                } catch (Exception ex1) {
                    throw new ServletException(ex1);
                }
            } catch (Exception e) {
                e.printStackTrace();
                response.setStatus(500);
                writer.writeError(response.getOutputStream(), e);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new ServletException(e);
        } finally {
            // close the fragment
            if (fragment != null) {
                try {
                    fragment.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }
}
