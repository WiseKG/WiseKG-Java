package org.wisekg.util;

import java.util.*;
import java.util.stream.Collectors;

import org.wisekg.main.Experiment;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import com.github.jsonldjava.shaded.com.google.common.collect.Sets;
import org.wisekg.model.*;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StarString;

public class QueryProcessingUtils {

    private static URLCodec urlCodec = new URLCodec("utf8");

    public static ArrayList<String> getBoundVariables(ArrayList<TriplePattern> triplePatterns) {
        ArrayList<String> boundedVariables = new ArrayList<String>();
        for (TriplePattern triplePattern : triplePatterns) {
            boundedVariables.addAll(triplePattern.getListOfVars());
        }
        return boundedVariables;
    }

    public static ArrayList<String> getBoundVariablesSP(ArrayList<StarPattern> starPatterns) {
        ArrayList<String> boundedVariables = new ArrayList<String>();
        for (StarPattern starPattern : starPatterns) {
            boundedVariables.addAll(starPattern.getListOfVars());
        }
        return boundedVariables;
    }

    public static TriplePattern findAndRemoveNextWithMaxNumberOfBV(
            ArrayList<TriplePattern> triplePatterns, ArrayList<String> boundVariables) {
        if (triplePatterns.isEmpty()) {
            return null;
        } else if (triplePatterns.size() == 1) {
            return triplePatterns.remove(0);
        }
        int minUBV = 4;
        int indexOfNextTP = 0;
        for (int i = 0; i < triplePatterns.size(); i++) {
            TriplePattern currTP = triplePatterns.get(i);
            int noOfV = currTP.getListOfVars().size() - currTP.getNumberOfBoundVariables(boundVariables);
            if (noOfV < minUBV) {
                minUBV = noOfV;
                indexOfNextTP = i;
            }
        }
        return triplePatterns.remove(indexOfNextTP);
    }

    public static StarPattern findAndRemoveNextWithMaxNumberOfBVSP(
            ArrayList<StarPattern> starPatterns, ArrayList<String> boundVariables) {
        if (starPatterns.isEmpty()) {
            return null;
        } else if (starPatterns.size() == 1) {
            return starPatterns.remove(0);
        }
        int maxNoOfBV = 0;
        int indexOfNextTP = 0;
        for (int i = 0; i < starPatterns.size(); i++) {
            StarPattern currTP = starPatterns.get(i);
            int noOfBV = currTP.getNumberOfBoundVariables(boundVariables);
            if (noOfBV > maxNoOfBV) {
                maxNoOfBV = noOfBV;
                indexOfNextTP = i;
            }
        }
        return starPatterns.remove(indexOfNextTP);
    }

    public static String constructFragmentURL(String startingFragment, TriplePattern tp, HashMap<Node, List> familiesHashedByPredicate)
            throws EncoderException {
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();

        isQuestionMarkAdded = appendUrlParam(sb, tp.getSubjectVar(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendUrlParam(sb, tp.getPredicateVar(),
                HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
        isQuestionMarkAdded =
                appendUrlParam(sb, tp.getObjectVar(), HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);

        return Config.getInstance().getDatasource() + sb.toString();
    }

    public static String constructStarURL(TriplePattern tp)
            throws EncoderException {
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();

        isQuestionMarkAdded = appendUrlParam(sb, tp.getSubjectVar(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendUrlParam(sb, tp.getPredicateVar(),
                HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
        isQuestionMarkAdded =
                appendUrlParam(sb, tp.getObjectVar(), HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);

        return sb.toString();
    }


    private static String getId(int familyId) {
        String id = "";

        id += Experiment.CLIENT_NUM;
        id += Experiment.QUERY;
        id += familyId;

        return id;
    }

    private static boolean hasInfrequent(StarPattern sp, HashMap<Node, List> familiesHashedByPredicate) {
        for(StatementPattern stmt : sp.getStatementPatterns()) {
            if(!familiesHashedByPredicate.containsKey(NodeFactory.createURI(stmt.getPredicateVar().getValue().stringValue()))) {
                return true;
            }
        }
        return false;
    }

    public static int getFamily(StarString sp, HashMap<Node, List> familiesHashedByPredicate) {
        List<Integer> familyList = getQueryStarsFamilies(sp, familiesHashedByPredicate);
        return familyList.size() > 0? familyList.get(0) : 0;
    }

    private static ArrayList<Integer> intersection(List<Integer> starQueryFamilies, List<Integer> tripleFamilyList) {
        // System.out.print("starQueryFamilies:" + starQueryFamilies.size());
        //  System.out.print("tripleFamilyList:" + tripleFamilyList.size());
        //System.out.println("==dsds==");
        if(tripleFamilyList == null) return new ArrayList<>(starQueryFamilies);
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

    public static List<Integer> getQueryStarsFamilies(StarString starPattern, HashMap<Node, List> familiesHashedByPredicate) {
        //todo Change if we need receive multiple files per star patterns
        List<Integer> queryStarsFamilies = new ArrayList<>();
        TripleString tp = starPattern.getTriple(0);
        List<Integer> starQueryFamilies = familiesHashedByPredicate.
                get(NodeFactory.createURI(tp.getPredicate().toString()));

        while(starQueryFamilies == null) {
            if(starPattern.size() == 1) return queryStarsFamilies;
            starPattern = new StarString(starPattern.getSubject(), new ArrayList<>(starPattern.getTriples()).subList(1,starPattern.size()));
            tp = starPattern.getTriple(0);
            starQueryFamilies = familiesHashedByPredicate.
                    get(NodeFactory.createURI(tp.getPredicate().toString()));
        }
        if (starQueryFamilies != null) {
            int size = starPattern.size();
            for(int i = 1; i < size; i++) {
                tp = starPattern.getTriple(i);
                starQueryFamilies = intersection(starQueryFamilies,
                        familiesHashedByPredicate.get(NodeFactory.createURI(tp.getPredicate().toString())));
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

                // todo Use SPF instead.
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

    private static boolean appendTripleParam(StringBuilder sb, int num, String paramName,
                                             Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(num);
        return isQuestionMarkAdded;
    }

    private static boolean appendUrlParam(StringBuilder sb, Var var, String paramName,
                                          Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (!var.isAnonymous()) {
                sb.append("&").append(paramName).append("=?").append(var.getName());
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("&").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
            }
        } else {
            if (!var.isAnonymous()) {
                sb.append("?").append(paramName).append("=?").append(var.getName());
                return true;
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("?").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private static boolean appendStringParam(StringBuilder sb, String str, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(urlCodec.encode(str));
        return isQuestionMarkAdded;
    }

    private static boolean appendUrlParamStar(StringBuilder sb, Var var, String paramName,
                                              Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (!var.isAnonymous()) {
                sb.append("&").append(paramName).append("=?").append(var.getName());
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("&").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
            }
        } else {
            if (!var.isAnonymous()) {
                sb.append("?").append(paramName).append("=?").append(var.getName());
                return true;
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("?").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private static boolean matchesWithBinding(TriplePattern tp, Statement triple,
                                              BindingHashMap binding) {
        String subjectVarName = tp.getSubjectVarName();
        if (binding.containsKey(subjectVarName)) {
            if (!binding.get(subjectVarName).getValue().equals(triple.getSubject().toString())) {
                return false;
            }
        }

        String predicateVarName = tp.getPredicateVarName();
        if (binding.containsKey(predicateVarName)) {
            if (!binding.get(predicateVarName).getValue().equals(triple.getPredicate().toString())) {
                return false;
            }
        }
        String objectVarName = tp.getObjectVarName();
        if (binding.containsKey(objectVarName)) {
            if (!binding.get(objectVarName).getValue().equals(triple.getObject().toString())) {
                return false;
            }

        }
        return true;

    }

    public static void extendBinding(TriplePattern tp, BindingHashMap binding, Statement triple) {
        String subjectVarName = tp.getSubjectVarName();
        if (subjectVarName != null && !binding.containsKey(subjectVarName)) {
            binding.put(subjectVarName,
                    new VarBinding(triple.getSubject().toString(), VarBinding.VarBindingType.IRI));
        }
        String predicateVarName = tp.getPredicateVarName();
        if (predicateVarName != null && !binding.containsKey(predicateVarName)) {
            binding.put(predicateVarName,
                    new VarBinding(triple.getPredicate().toString(), VarBinding.VarBindingType.IRI));
        }
        String objectVarName = tp.getObjectVarName();
        if (objectVarName != null && !binding.containsKey(objectVarName)) {
            if (triple.getObject() instanceof Literal) {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBinding.VarBindingType.LITERAL));
            } else {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBinding.VarBindingType.IRI));
            }
        }
    }

    public static BindingHashMap createBinding(TriplePattern tp, Statement triple) {
        BindingHashMap binding = new BindingHashMap();
        String subjectVarName = tp.getSubjectVarName();
        if (subjectVarName != null) {
            binding.put(subjectVarName,
                    new VarBinding(triple.getSubject().toString(), VarBinding.VarBindingType.IRI));
        }
        String predicateVarName = tp.getPredicateVarName();
        if (predicateVarName != null) {
            binding.put(predicateVarName,
                    new VarBinding(triple.getPredicate().toString(), VarBinding.VarBindingType.IRI));
        }
        String objectVarName = tp.getObjectVarName();
        if (objectVarName != null) {
            if (triple.getObject() instanceof Literal) {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBinding.VarBindingType.LITERAL));
            } else {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBinding.VarBindingType.IRI));
            }
        }
        return binding;
    }

    public static void extendBinding(BindingHashMap firstBHM, BindingHashMap secondBHM) {
        Set<String> secondVarNames = secondBHM.keySet();
        Set<String> firstVarNames = firstBHM.keySet();
        Set<String> differentVarNames = Sets.difference(secondVarNames, firstVarNames);
        for (String differentVarName : differentVarNames) {
            firstBHM.put(differentVarName, secondBHM.get(differentVarName));
        }
    }

    public static ArrayList<BindingHashMap> extendBindings(ArrayList<BindingHashMap> bindings,
                                                           TriplePattern tp, Collection<Statement> triples) {
        ArrayList<BindingHashMap> extendedBindings = new ArrayList<BindingHashMap>();
        if (bindings.isEmpty()) {
            for (Statement triple : triples) {
                BindingHashMap binding = new BindingHashMap();
                extendBinding(tp, binding, triple);
                extendedBindings.add(binding);
            }
        } else {
            for (BindingHashMap currentBinding : bindings) {
                for (Statement triple : triples) {
                    if (matchesWithBinding(tp, triple, currentBinding)) {
                        BindingHashMap newBinding = new BindingHashMap(currentBinding);
                        extendBinding(tp, newBinding, triple);
                        extendedBindings.add(newBinding);
                        //break;
                    }
                }
            }

        }
        return extendedBindings;
    }

    private static boolean matchesWithBinding(StarString tp, List<Statement> triple,
                                              BindingHashMap binding) {
        String subjectVarName = tp.getSubject().toString();
        if (binding.containsKey(subjectVarName)) {
            if (!binding.get(subjectVarName).getValue().equals(triple.get(0).getSubject().toString())) {
                return false;
            }
        }

        int cnt = tp.size();
        for (int i = 0; i < cnt; i++) {
            TripleString stp = tp.getTriple(i);
            String predVal = stp.getPredicate().toString();
            Statement stmt = null;
            for (Statement s : triple) {
                if (predVal.equals(s.getPredicate().toString())) {
                    stmt = s;
                    break;
                }
            }
            if (stmt == null) continue;

            String predicateVarName = stp.getPredicate().toString();
            if (binding.containsKey(predicateVarName)) {
                if (!binding.get(predicateVarName).getValue().equals(stmt.getPredicate().toString())) {
                    return false;
                }
            }
            String objectVarName = stp.getObject().toString();
            if (binding.containsKey(objectVarName)) {
                if (!binding.get(objectVarName).getValue().equals(stmt.getObject().toString())) {
                    return false;
                }
            }
        }
        return true;

    }

    public static void extendBinding(StarString sp, BindingHashMap binding, List<Statement> stars) {
        String subjectVarName = sp.getSubject().toString();
        if (subjectVarName.startsWith("?") && !binding.containsKey(subjectVarName)) {
            binding.put(subjectVarName,
                    new VarBinding(stars.get(0).getSubject().toString(), VarBinding.VarBindingType.IRI));
        }

        int cnt = sp.size();
        for (int i = 0; i < cnt; i++) {
            TripleString stp = sp.getTriple(i);
            Statement stmt = null;
            for (Statement s : stars) {
                if (stp.getPredicate().toString().equals(s.getPredicate().toString())) {
                    stmt = s;
                    break;
                }
            }
            if (stmt == null) continue;
            String predicateVarName = stp.getPredicate().toString();
            if (predicateVarName.startsWith("?") && !binding.containsKey(predicateVarName)) {
                binding.put(predicateVarName,
                        new VarBinding(stmt.getPredicate().toString(), VarBinding.VarBindingType.IRI));
            }
            String objectVarName = stp.getObject().toString();
            if (objectVarName.startsWith("?") && !binding.containsKey(objectVarName)) {
                if (stmt.getObject() instanceof Literal) {
                    binding.put(objectVarName,
                            new VarBinding(stmt.getObject().toString(), VarBinding.VarBindingType.LITERAL));
                } else {
                    binding.put(objectVarName,
                            new VarBinding(stmt.getObject().toString(), VarBinding.VarBindingType.IRI));
                }
            }
        }
    }

    public static ArrayList<BindingHashMap> extendBindings(ArrayList<BindingHashMap> bindings,
                                                           StarString sp, List<List<Statement>> stars) {
        ArrayList<BindingHashMap> extendedBindings = new ArrayList<>();
        if (bindings.isEmpty()) {
            for (List<Statement> triple : stars) {
                BindingHashMap binding = new BindingHashMap();
                extendBinding(sp, binding, triple);
                extendedBindings.add(binding);
            }
        } else {
            for (BindingHashMap currentBinding : bindings) {
                for (List<Statement> triple : stars) {
                    if (matchesWithBinding(sp, triple, currentBinding)) {
                        BindingHashMap newBinding = new BindingHashMap(currentBinding);
                        extendBinding(sp, newBinding, triple);
                        extendedBindings.add(newBinding);
                    }
                }
            }

        }
        return extendedBindings;
    }

    public static BindingHashMap extendBindingWithSingleTriple(BindingHashMap currentBinding,
                                                               TriplePattern tp, Statement triple) {
        if (currentBinding == null) {
            BindingHashMap extendedBinding = new BindingHashMap();
            extendBinding(tp, extendedBinding, triple);
            return extendedBinding;
        } else {
            if (matchesWithBinding(tp, triple, currentBinding)) {
                BindingHashMap newBinding = new BindingHashMap(currentBinding);
                extendBinding(tp, newBinding, triple);
                return newBinding;
            } else {
                return null;
            }
        }
    }
}
