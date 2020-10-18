package org.linkeddatafragments.fragments.spf;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.StarPatternElementParser;
import org.linkeddatafragments.util.Tuple;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SPFRequestParser<ConstantTermType,NamedVarType,AnonVarType>
        extends FragmentRequestParserBase {
    private int current = 0;
    public final StarPatternElementParser<ConstantTermType,NamedVarType,AnonVarType> elmtParser;

    /**
     *
     * @param elmtParser
     */
    public SPFRequestParser(
            final StarPatternElementParser<ConstantTermType,NamedVarType,AnonVarType> elmtParser )
    {
        this.elmtParser = elmtParser;
    }

    /**
     *
     * @param httpRequest
     * @param config
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getWorker(final HttpServletRequest httpRequest,
                               final ConfigReader config )
            throws IllegalArgumentException
    {
        return new Worker( httpRequest, config );
    }

    /**
     *
     */
    protected class Worker extends FragmentRequestParserBase.Worker
    {

        /**
         *
         * @param request
         * @param config
         */
        public Worker( final HttpServletRequest request,
                       final ConfigReader config )
        {
            super( request, config );
        }

        /**
         *
         * @return
         * @throws IllegalArgumentException
         */
        @Override
        public ILinkedDataFragmentRequest createFragmentRequest()
                throws IllegalArgumentException
        {
            // System.out.println("Create Fragment Request :)");
            return new StarPatternFragmentRequestImpl<ConstantTermType,NamedVarType,AnonVarType>(
                    getFragmentURL(),
                    getDatasetURL(),
                    pageNumberWasRequested,
                    pageNumber,
                    getSubject(),
                    getStars(),
                    getBindings(),
                    getNumTriples(),
                    getRequestHash());
        }

        private long getRequestHash() {
            String s = request.getParameter("s");
            long sHash = 0;
            if(s != null)
                sHash = s.hashCode();

            String star = request.getParameter("star");
            long starHash = 0;
            if(star != null)
                starHash = star.hashCode();

            String bindings = request.getParameter("values");
            long bindingsHash = 0;
            if(bindings != null)
                bindingsHash = bindings.hashCode();

            return sHash + starHash + bindingsHash;
        }

        /**
         *
         * @return
         */
        public IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType> getSubject() {
            if(request.getParameter("s") == null && request.getParameter("subject") == null) {
                int old = current;
                current++;
                return elmtParser.parseIntoStarPatternElement("?var"+old);
            } else if(request.getParameter("s") == null ) {
                String elem = request.getParameter("subject");
                if(elem.equals("")) {
                    int old = current;
                    current++;
                    return elmtParser.parseIntoStarPatternElement("?var"+old);
                }
                return getParameterAsStarPatternElement("subject");
            }
            String elem = request.getParameter("s");
            if(elem.equals("")) {
                int old = current;
                current++;
                return elmtParser.parseIntoStarPatternElement("?var"+old);
            }
            return getParameterAsStarPatternElement("s");
        }

        /**
         *
         * @return
         */
        public List<Tuple<IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>,
                IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>>> getStars() {
            List<Tuple<IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>,
                    IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>>> lst = new ArrayList<>();

            String starString = request.getParameter("star").replace("[", "").replaceAll("]", "");
            String[] elements = starString.split(";");

            Map<String, String> map = new HashMap<>();

            for(String s : elements) {
                String[] ele = s.split(",");
                String str = ele[1];

                for(int i = 2; i < ele.length; i++) {
                    str += "," + ele[i];
                }

                map.put(ele[0], str);
            }

            int triples = getNumTriples();
            for(int i = 0; i < triples; i++) {
                int n = i+1;
                String pkey = "p"+n;
                String okey = "o"+n;
                String p, o;
                if(map.containsKey(pkey) && !map.get(pkey).equals(""))
                    p = map.get(pkey);
                else {
                    p = "?var" + current;
                    current++;
                }
                if(map.containsKey(okey) && !map.get(okey).equals(""))
                    o = map.get(okey);
                else {
                    o = "?var" + current;
                    current++;
                }

                lst.add(new Tuple<>(getStringAsStarPatternElement(p),
                        getStringAsStarPatternElement(o)));
            }

            return lst;
        }

        public List<Binding> getBindings() {
            final List<Var> foundVariables = new ArrayList<Var>();
            return parseAsSetOfBindings(
                    request.getParameter("values"),
                    foundVariables);
        }

        private IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>
        getStringAsStarPatternElement(final String ele )
        {
            return elmtParser.parseIntoStarPatternElement( ele );
        }

        /**
         *
         * @param paramName
         * @return
         */
        public IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>
        getParameterAsStarPatternElement(final String paramName )
        {
            final String parameter = request.getParameter( paramName );
            return elmtParser.parseIntoStarPatternElement( parameter );
        }

        private int getNumTriples() {
            return Integer.parseInt(request.getParameter("triples"));
        }

        /**
         * Parses the given value as set of bindings.
         *
         * @param value          containing the SPARQL bindings
         * @param foundVariables a list with variables found in the VALUES clause
         * @return a list with solution mappings found in the VALUES clause
         */
        private List<Binding> parseAsSetOfBindings(final String value, final List<Var> foundVariables) {
            if (value == null) {
                return null;
            }
            String newString = "select * where {} VALUES " + value;
            Query q = QueryFactory.create(newString);
            foundVariables.addAll(q.getValuesVariables());
            return q.getValuesData();
        }

    } // end of class Worker
}
