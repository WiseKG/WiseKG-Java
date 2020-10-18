package org.linkeddatafragments.servlet;

import com.google.gson.Gson;
import org.apache.http.HttpHeaders;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.Lang;
import org.linkeddatafragments.costmodel.ICostModel;
import org.linkeddatafragments.costmodel.impl.FamilyShippingCostModel;
import org.linkeddatafragments.costmodel.impl.StarCostModel;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.executionplan.QueryExecutionPlan;
import org.linkeddatafragments.executionplan.QueryOperator;
import org.linkeddatafragments.queryAnalyzer.Config;
import org.linkeddatafragments.queryAnalyzer.FamiliesConfig;
import org.linkeddatafragments.queryAnalyzer.Family;
import org.linkeddatafragments.util.BgpStarPatternVisitor;
import org.linkeddatafragments.util.MIMEParse;
import org.linkeddatafragments.util.StarString;
import org.linkeddatafragments.util.Tuple;
import org.linkeddatafragments.views.ILinkedDataFragmentWriter;
import org.linkeddatafragments.views.LinkedDataFragmentWriterFactory;
import org.rdfhdt.hdt.triples.TripleString;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class QueryExecutionPlanServlet extends HttpServlet {
    private final static long serialVersionUID = 1L;
    private static HashMap<Node, List> familiesHashedByPredicate = null;
    private static long quantum = 300000L;

    // Parameters
    /**
     * baseURL
     */
    public final static String CFGFILE = "configFile";

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
            PartitioningServlet.config = new ConfigReader(new FileReader(configFile));

            // register content types
            //MIMEParse.register("text/html");
            //MIMEParse.register(Lang.RDFXML.getHeaderString());
            //MIMEParse.register(Lang.NTRIPLES.getHeaderString());
            //MIMEParse.register(Lang.JSONLD.getHeaderString());
            MIMEParse.register(Lang.TTL.getHeaderString());

            Config config = new Config(configFile.getAbsolutePath());
            familiesHashedByPredicate = FamiliesConfig.getInstance().getFamiliesByPredicate();
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    /**
     *
     */
    @Override
    public void destroy() {
    }

    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        try {
            String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
            String bestMatch = MIMEParse.bestMatch(acceptHeader);

            response.setHeader(HttpHeaders.SERVER, "Linked Data Fragments Server");
            response.setContentType(bestMatch);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());



            String bgpString = request.getParameter("bgp");
            if (bgpString == null) throw new ServletException("BGP not specified.");

            String qStr = "SELECT * WHERE { " + bgpString + "}";
            Query q = QueryFactory.create(qStr);
            BgpStarPatternVisitor visitor = new BgpStarPatternVisitor();
            q.getQueryPattern().visit(visitor);

            List<StarString> stars = new ArrayList<>(visitor.getStarPatterns());

            long s = System.currentTimeMillis();
            QueryExecutionPlan plan = createExecutionPlan(stars, QueryExecutionPlan.getNullPlan(), request, new ArrayList<>());

            Gson gson = new Gson();
            response.getWriter().println(gson.toJson(plan));
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private StarString getFirstStar(List<StarString> stars) {
        if(stars.size() == 0) return null;
        if(stars.size() == 1) return stars.get(0);

        StarString best = null;
        long low = Long.MAX_VALUE;
        for(StarString star : stars) {
            IDataSource ds;
            if(star.size() == 1 || hasInfrequent(star)) {
                try {
                    ds = LinkedDataFragmentServlet.getDefaultDatasource();
                } catch (DataSourceNotFoundException e) {
                    continue;
                }
            } else {
                int family = getQueryStarsFamilies(star).get(0);
                try {
                    ds = PartitioningServlet.getDataSource(PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");
                } catch (DataSourceNotFoundException e) {
                    continue;
                }
            }

            long card = ds.cardinalityEstimation(star);
            //if(card == 0) return null;
            if(card < low) {
                low = card;
                best = star;
            }
        }
        return best;
    }

    private StarString getNextStar(List<StarString> stars, List<String> boundVars) {
        if(stars.size() == 0) return null;
        if(stars.size() == 1) return stars.get(0);

        StarString best = null;
        int max = -1;
        int maxBSO = -1;
        for(StarString star : stars) {
            int bv = star.numBoundVars(boundVars);
            int bso = star.numBoundSO(boundVars);
            if(bv > max || (bv == max && bso > maxBSO)) {
                max = bv;
                maxBSO = bso;
                best = star;
            }
        }
        return best;
    }

    private QueryExecutionPlan createExecutionPlan(List<StarString> stars, QueryExecutionPlan subplan, HttpServletRequest request, List<String> boundVars) {
        if(stars.size() == 0) return subplan;
        StarString next = null;

        System.out.println(stars.toString());
        if(subplan.isNullPlan()) {
            next = getFirstStar(stars);
        } else {
            next = getNextStar(stars, boundVars);
        }

        if(next == null) return subplan;

        System.out.println(next.toString());

        boundVars.addAll(next.getVariables());

        int family = 0;
        IDataSource datasource;
        String partitionUrl;
        if(next.size() == 1 || hasInfrequent(next)) {
            QueryOperator operator = new QueryOperator(PartitioningServlet.config.getUri() + PartitioningServlet.config.getDefaultGraph(), next);
            List<StarString> lst = new ArrayList<>(stars);
            lst.remove(next);
            QueryExecutionPlan p = new QueryExecutionPlan(operator, subplan, System.currentTimeMillis() + quantum);
            return createExecutionPlan(lst, p, request, boundVars);
        } else {
            try {
                family = getQueryStarsFamilies(next).get(0);
                System.out.println(family);
                partitionUrl = PartitioningServlet.config.getUri() + "partition/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt";
                datasource = PartitioningServlet.getDataSource(PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt");
            } catch (DataSourceNotFoundException e) {
                System.out.println("Error getting datasource/family");
                return QueryExecutionPlan.getNullPlan();
            }
        }

        final ICostModel shippingCostModel = new FamilyShippingCostModel(PartitioningServlet.config, next, subplan);
        final ICostModel localCostModel = new StarCostModel(next, subplan);

        double shipping = shippingCostModel.cost(request, datasource);
        double local = localCostModel.cost(request, datasource);

        QueryOperator op;
        if(local <= shipping) {
            op = new QueryOperator(partitionUrl, next);
        } else {
            op = new QueryOperator(PartitioningServlet.config.getUri() + "molecule/" + PartitioningServlet.config.getPartitionstring() + "_" + family + ".hdt", next);
        }

        QueryExecutionPlan p = new QueryExecutionPlan(op, subplan, System.currentTimeMillis() + quantum);
        List<StarString> lst = new ArrayList<>(stars);
        lst.remove(next);
        return createExecutionPlan(lst, p, request, boundVars);
    }

    private boolean hasInfrequent(StarString sp) {
        for (Tuple<CharSequence, CharSequence> stmt : sp.getTriples()) {
            if (!familiesHashedByPredicate.containsKey(NodeFactory.createURI(stmt.x.toString()))) {
                return true;
            }
        }
        return false;
    }

    private ArrayList<Integer> intersection(List<Integer> starQueryFamilies, List<Integer> tripleFamilyList) {
        // System.out.print("starQueryFamilies:" + starQueryFamilies.size());
        //  System.out.print("tripleFamilyList:" + tripleFamilyList.size());
        //System.out.println("==dsds==");
        if (tripleFamilyList == null) return new ArrayList<>(starQueryFamilies);
        int i = 0;
        int j = 0;
        ArrayList<Integer> intersectedList = new ArrayList<>();
        while (i < starQueryFamilies.size() && j < tripleFamilyList.size()) {
            // System.out.println("==tripleFamilyList==");
            if (starQueryFamilies.get(i) < tripleFamilyList.get(j)) {
                i++;// Increase I move to next element
            } else if (tripleFamilyList.get(j) < starQueryFamilies.get(i)) {
                j++;// Increase J move to next element
            } else {
                intersectedList.add(tripleFamilyList.get(j));
                i++;// If same increase I & J both
            }
        }
        // System.out.println("intersectedList: " + intersectedList.size());
        return intersectedList;
    }

    private List<Integer> getQueryStarsFamilies(StarString starPattern) {
        //todo Change if we need receive multiple files per star patterns
        List<Integer> queryStarsFamilies = new ArrayList<>();
        TripleString stmtPattern = starPattern.getTriple(0);
        List<Integer> starQueryFamilies = familiesHashedByPredicate.
                get(NodeFactory.createURI(stmtPattern.getPredicate().toString()));

        while (starQueryFamilies == null) {
            if (starPattern.size() == 1) return queryStarsFamilies;
            starPattern = new StarString(stmtPattern.getSubject(), starPattern.getTriples().subList(1, starPattern.getTriples().size()));
            stmtPattern = starPattern.getTriple(0);
            starQueryFamilies = familiesHashedByPredicate.
                    get(NodeFactory.createURI(stmtPattern.getPredicate().toString()));
        }
        if (starQueryFamilies != null) {
            int size = starPattern.size();
            for (int i = 1; i < size; i++) {
                TripleString stmt = starPattern.getTriple(i);
                starQueryFamilies = intersection(starQueryFamilies,
                        familiesHashedByPredicate.get(NodeFactory.createURI(stmt.getPredicate().toString())));
            }
            List<Integer> starQueryGroupedFamilies = starQueryFamilies.stream().filter(familyId
                    -> FamiliesConfig.getInstance().getFamilyByID(familyId).isGrouped()).collect(Collectors.toList());
            if (starQueryGroupedFamilies.isEmpty()) {
                queryStarsFamilies.addAll(starQueryFamilies);
            } else if (starQueryGroupedFamilies.size() == 1) {
                queryStarsFamilies.addAll(starQueryGroupedFamilies);
            } else if (starQueryGroupedFamilies.size() > 1) {
                Optional<Integer> PartitionId = starQueryGroupedFamilies.stream().min((f1, f2) -> Integer.compare(
                        FamiliesConfig.getInstance().getFamilyByID(f1).getPredicateSet().size(),
                        FamiliesConfig.getInstance().getFamilyByID(f2).getPredicateSet().size()));
                Family familySolution = FamiliesConfig.getInstance().getFamilyByID(PartitionId.get());

                List<Integer> sourceSet = familySolution.getSourceSet();

                if (sourceSet != null) {
                    if (familySolution.isOriginalFamily()) {
                        sourceSet.add(familySolution.getIndex());
                    }
                    queryStarsFamilies.addAll(sourceSet);
                } else {
                    queryStarsFamilies.addAll(new ArrayList<>(Arrays.asList(PartitionId.get())));
                }
            } else {
                // Todo remove this part
                //System.out.println("1 predicate star");
                List<Integer> starQueryDetailedFamilies = starQueryFamilies.stream().filter(
                        familyId -> !(FamiliesConfig.getInstance().getFamilyByID(familyId)).isGrouped()).collect(Collectors.toList());
                queryStarsFamilies.addAll(starQueryDetailedFamilies);
            }
        }
        return queryStarsFamilies;
    }
}
