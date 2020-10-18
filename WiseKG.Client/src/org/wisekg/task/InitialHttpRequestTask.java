package org.wisekg.task;

import org.wisekg.util.Config;
import org.apache.commons.codec.EncoderException;
import org.wisekg.model.TriplePattern;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.query.algebra.Var;

import java.util.*;

public class InitialHttpRequestTask {
    private List<TriplePattern> triplePatterns;
    private String fragmentURL;
    private static URLCodec urlCodec = new URLCodec("utf8");

    public InitialHttpRequestTask(List<TriplePattern> triplePatterns) {
        this.triplePatterns = triplePatterns;
        try {
            this.fragmentURL = constructFragmentURL();
        } catch (EncoderException e) {
            e.printStackTrace();
        }
    }

    private String constructFragmentURL() throws EncoderException {
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();

        StringBuilder sbBgp = new StringBuilder();
        for(TriplePattern tp : triplePatterns) {


            String subj, pred, obj;
            Var sVar = tp.getSubjectVar(), pVar = tp.getPredicateVar(), oVar = tp.getObjectVar();

            if(sVar.getValue() != null)
                subj = "<" + sVar.getValue().stringValue() + ">";
            else
                subj = "?"+sVar.getName();

            if(pVar.getValue() != null)
                pred = "<" + pVar.getValue().stringValue() + ">";
            else
                pred = "?"+pVar.getName();

            if(oVar.getValue() != null)
                obj = "<" + oVar.getValue().stringValue() + ">";
            else {
                String v = oVar.getName();
                if(v.startsWith("_anon_"))
                    obj = "?" + sVar.getName();
                else
                    obj = "?" + oVar.getName();
            }

            sbBgp.append(subj + " " + pred + " " + obj + " . ");
        }

        isQuestionMarkAdded = appendStringParam(sb, sbBgp.toString(), "bgp", isQuestionMarkAdded);
        isQuestionMarkAdded = appendStringParam(sb, Config.getInstance().getSpeed() + "", "speed", isQuestionMarkAdded);
        isQuestionMarkAdded = appendStringParam(sb, Config.getInstance().getLatency() + "", "latency", isQuestionMarkAdded);
        isQuestionMarkAdded = appendStringParam(sb, Config.getInstance().getIps() + "", "ips", isQuestionMarkAdded);
        return Config.getInstance().getPlanner() + sb.toString();
    }

    private boolean appendStringParam(StringBuilder sb, String str, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            sb.append("&").append(paramName).append("=").append(urlCodec.encode(str));
        } else {
            sb.append("?").append(paramName).append("=").append(urlCodec.encode(str));
        }
        return true;
    }

    public String getFragmentURL() {
        return fragmentURL;
    }
}
