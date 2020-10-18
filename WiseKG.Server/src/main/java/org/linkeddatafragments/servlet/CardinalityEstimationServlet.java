package org.linkeddatafragments.servlet;

import com.google.gson.JsonObject;
import org.apache.http.HttpHeaders;
import org.apache.jena.riot.Lang;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.*;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CardinalityEstimationServlet extends HttpServlet {
    private final static long serialVersionUID = 1L;

    // Parameters
    /**
     * baseURL
     */
    public final static String CFGFILE = "configFile";

    private ConfigReader config;
    private final HashMap<String, IDataSource> dataSources = new HashMap<>();
    private final Collection<String> mimeTypes = new ArrayList<>();

    private File getConfigFile(ServletConfig config) throws IOException {
        String path = config.getServletContext().getRealPath("/");

        if (path == null) {
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
            // load the configuration
            File configFile = getConfigFile(servletConfig);
            config = new ConfigReader(new FileReader(configFile));

            // register data sources
            for (Map.Entry<String, JsonObject> dataSource : config.getDataSources().entrySet()) {
                dataSources.put(dataSource.getKey(), DataSourceFactory.create(dataSource.getValue()));
            }

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
     *
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

    /**
     * Get the datasource
     *
     * @param request
     * @return
     * @throws IOException
     */
    private IDataSource getDataSource(HttpServletRequest request) throws DataSourceNotFoundException {
        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

        //  System.out.println("contextPath =======>" + contextPath);
        //  System.out.println("requestURI =======>" + requestURI);
        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());
        String dataSourceName = path.substring(path.lastIndexOf("/")+1);
        // System.out.println("dataSourceName =======> " + dataSourceName);
        IDataSource dataSource = dataSources.get(dataSourceName);
        if (dataSource == null) {
            throw new DataSourceNotFoundException(dataSourceName);
        }
        return dataSource;
    }

    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        ILinkedDataFragment fragment = null;
        try {

          /*  if (request.getParameter("stats") != null) {
                response.getWriter().write("HI FROM STATISTICS");
                return;
            }*/
            //   System.out.println("===================Hi Javier===================");
            // do conneg
            String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
            String bestMatch = MIMEParse.bestMatch(acceptHeader);
            //   System.out.println("Accept Header ==>" + acceptHeader);
            //   System.out.println("Best Match==>" + acceptHeader);

            // set additional response headers
            response.setHeader(HttpHeaders.SERVER, "Linked Data Fragments Server");
            response.setContentType(bestMatch);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());

            ILinkedDataFragmentWriter writer = LinkedDataFragmentWriterFactory.create(config.getPrefixes(), dataSources, bestMatch);

            try {

                final IDataSource dataSource = getDataSource(request);

                final ILinkedDataFragmentRequest ldfRequest;

                if (request.getParameter("triples") == null) {
                    if (request.getParameter("values") == null) {
                        ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.TPF)
                                .parseIntoFragmentRequest(request, config);
                        fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.TPF)
                                .createRequestedMetadataFragment(ldfRequest);
                    } else {
                        ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.BRTPF)
                                .parseIntoFragmentRequest(request, config);
                        fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.BRTPF)
                                .createRequestedMetadataFragment(ldfRequest);
                    }
                } else {
                    ldfRequest = dataSource.getRequestParser(IDataSource.ProcessorType.SPF)
                            .parseIntoFragmentRequest(request, config);
                    fragment = dataSource.getRequestProcessor(IDataSource.ProcessorType.SPF)
                            .createRequestedMetadataFragment(ldfRequest);
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
