package org.wisekg.main;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.wisekg.model.StarPattern;
import org.wisekg.util.Config;
import org.wisekg.util.FamiliesConfig;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Node;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;
import org.wisekg.model.TriplePattern;

public class Experiment {

    private static List<ProjectionElem> projectionElemList;
    private static ArrayList<TriplePattern> triplePatterns;
    private static ArrayList<StarPattern> starPatterns;
    private static String queryStr;
    private static HashMap<Node, List> familiesHashedByPredicate;
    public static int CLIENT_NUM = 0;
    public static String QUERY = "";
    private static Map<String, String> map = new HashMap<>();
    public static ArrayList<Integer> cardinalities = new ArrayList<>();

    public static void main(String[] args)
            throws IllegalArgumentException, IOException, InterruptedException, ExecutionException {
        if (!(args.length == 7)) {
            System.out.println("Usage: java -jar [filename].jar [query directory] [method] [output dir] [number of clients] [client no.] [dataset] [load]");
            return;
        }

        String configf = args[0];
        String queryDir = args[1];
        String method = args[2];
        String outDir = args[3];
        int num_clients = Integer.parseInt(args[4]);
        int client_num = Integer.parseInt(args[5]);
        String dataset = args[6];
        int executing = 0;
        //String validDir = args[7];

        CLIENT_NUM = client_num;

        Config config = new Config(configf);

        familiesHashedByPredicate = FamiliesConfig.getInstance().getFamiliesByPredicate();

        String oDir = outDir + "/" + dataset + "/" + num_clients + "_clients/" + method;
        new File(oDir).mkdirs();
        String filename = oDir + "/client" + client_num + ".csv";
        FileWriter writer = new FileWriter(filename);

        File[] dirs = new File(queryDir).listFiles();
        for (File qf : dirs) {
            System.out.println(qf.getName());

            QUERY = qf.getName();
            initializeQueryAndConfig(qf.getPath());

            QueryInput input = new QueryInput();
            input.setStartFragment(Config.getInstance().getServer());

            SparqlQueryProcessor sqp = new SparqlQueryProcessor(triplePatterns, input, false, false);

            final Duration timeout = Duration.ofMinutes(5);
            ExecutorService executor = Executors.newSingleThreadExecutor();

            final Future handler = executor.submit(new Callable() {
                @Override
                public String call() throws Exception {
                    sqp.processQuery();
                    return "";
                }
            });

            try {
                executing++;
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = qf.getName() + ";" + SparqlQueryProcessor.SERVER_REQUESTS.get() + ";" + sqp.getQueryProcessingTime() + ";" + SparqlQueryProcessor.TRANSFERRED_BYTES.get() + ";" + SparqlQueryProcessor.RESPONSE_TIME + ";" + SparqlQueryProcessor.NUMBER_OF_OUTPUT_BINDINGS;
                System.out.println(str);
                writer.write(str + "\n");
            } catch (TimeoutException | InterruptedException e) {
                handler.cancel(true);
                sqp.terminate();

                String str = qf.getName() + ";-1;-1;-1;-1;-1";
                System.out.println(str);
                writer.write(str + "\n");
            } finally {
                executing--;
                executor.shutdownNow();
                sqp.close();
            }
        }
        writer.close();

        if(executing == 0) System.exit(0);
    }

    private static void initializeQueryAndConfig(String queryFile)
            throws IOException, IllegalArgumentException {
        triplePatterns = new ArrayList<TriplePattern>();
        String queryString = FileUtils.readFileToString(new File(queryFile), StandardCharsets.UTF_8);
        queryStr = queryString;
        SPARQLParserFactory factory = new SPARQLParserFactory();
        QueryParser parser = factory.getParser();
        ParsedQuery parsedQuery = parser.parseQuery(queryString, null);
        TupleExpr query = parsedQuery.getTupleExpr();
        if (query instanceof Projection) {
            Projection proj = (Projection) query;
            projectionElemList = proj.getProjectionElemList().getElements();
        } else {
            throw new IllegalArgumentException("The given query should be a select query.");
        }

        List<StatementPattern> statementPatterns = StatementPatternCollector.process(query);
        Map<String, List<StatementPattern>> patterns = new HashMap<>();
        for (StatementPattern statementPattern : statementPatterns) {
            TriplePattern tp = new TriplePattern(statementPattern);

            triplePatterns.add(tp);
            String subj = tp.getSubjectVarName();

            if(patterns.containsKey(subj)) {
                patterns.get(subj).add(statementPattern);
            } else {
                List<StatementPattern> lst = new ArrayList<>();
                lst.add(statementPattern);
                patterns.put(subj, lst);
            }
        }

        starPatterns = new ArrayList<>();
        Collection<List<StatementPattern>> lst = patterns.values();
        for(List<StatementPattern> stps : lst) {
            starPatterns.add(new StarPattern(stps));
        }
    }
}
