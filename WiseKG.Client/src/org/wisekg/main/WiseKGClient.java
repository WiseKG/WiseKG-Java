package org.wisekg.main;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.wisekg.callable.WiseKGHttpRequestThread;
import org.wisekg.util.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;
import org.wisekg.main.QueryInput.QueryProcessingMethod;
import org.wisekg.model.TriplePattern;

public class WiseKGClient {
    private static ArrayList<TriplePattern> triplePatterns = new ArrayList<TriplePattern>();
    private static List<ProjectionElem> projectionElemList;
    private static QueryInput input;
    private static QueryProcessingMethod qpMethod = QueryProcessingMethod.SPF;
    private static String queryStr = "";
    private static boolean tests = false;
    private static Config config = null;

    public static void main(String[] args) throws IOException {
        try {
            initializeInput(args);
            if(tests) {
                Experiment.main(Arrays.copyOfRange(args, 2, args.length-1));
                return;
            }
            initializeQueryAndConfig();
            SparqlQueryProcessor sqp =
                    new SparqlQueryProcessor(triplePatterns, input, false, true);
            sqp.processQuery();
            sqp.printBindings();

            System.out.println(SparqlQueryProcessor.SERVER_REQUESTS.get() + " " + SparqlQueryProcessor.TRANSFERRED_BYTES.get() + " " + SparqlQueryProcessor.RESPONSE_TIME);
            System.out.println(WiseKGHttpRequestThread.currResStr);
            System.out.println((System.currentTimeMillis()-SparqlQueryProcessor.START_TIME));
        } catch (ParseException e) {
            System.err.println("usage: java skytpf-client.jar -f startFragment -q query.sparql");

        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void initializeQueryAndConfig() throws IOException, IllegalArgumentException {
        String queryString =
                FileUtils.readFileToString(new File(input.getQueryFile()), StandardCharsets.UTF_8);
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
        }
    }

    private static void initializeInput(String[] args)
            throws ParseException, IllegalArgumentException {
        Option optionQ =
                Option.builder("q").required(false).desc("SPARQL query file").longOpt("query").build();
        optionQ.setArgs(1);
        Option optionT =
                Option.builder("t").required(true).desc("Run tests").longOpt("tests").build();
        optionT.setArgs(1);
        Option optionC =
                Option.builder("c").required(true).desc("Config file").longOpt("config").build();
        optionC.setArgs(1);
        Options options = new Options();
        options.addOption(optionQ);
        options.addOption(optionC);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        //String s = commandLine.getOptionValue("t");
        //if(s.equals("true")) {
        //    tests = true;
        //    return;
        // }

        input = new QueryInput();
        input.setQueryFile(commandLine.getOptionValue("q"));
        config = new Config(commandLine.getOptionValue("c"));
        input.setStartFragment(Config.getInstance().getServer());
    }
}
