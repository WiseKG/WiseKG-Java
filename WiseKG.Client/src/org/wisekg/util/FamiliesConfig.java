package org.wisekg.util;

import com.google.gson.Gson;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public class FamiliesConfig {

    private static FamiliesConfig fc;
    private int numFamilies;
    private List<Family> families;

    private FamiliesConfig() {
    }

    public static FamiliesConfig getInstance() {

        if (fc == null) {
            Gson gson = new Gson();
            //Load from local

            Path metaDatapath = Paths.get(Config.getInstance().getMetadatapath());

            File metaData = new File(metaDatapath.toString());

            if (metaData.exists()) {
                // System.out.println("metaData");
                try {
                    fc = gson.fromJson(new FileReader(metaData), FamiliesConfig.class);
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(FamiliesConfig.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                // Todo Do later to access the server
                /*//System.out.println("ServerInteractionHandler");
                //read from Server
                ServerInteractionHandler sih = new ServerInteractionHandler();

                String sb = sih.getServerFamiliesMetaData();
                // System.out.println("SB ==>" + sb.length());
                //Use try-with-resource to get auto-closeable writer instance

                fc = gson.fromJson(sb, FamiliesConfig.class);*/
            }
        }

        return fc;
    }

    public int getNumFamilies() {
        return numFamilies;
    }

    public List<Family> getFamilies() {
        return families;
    }

    public Family getFamilyByID(int ID) {
        return families.get(ID - 1);
    }

    public HashMap<Node, List> getFamiliesByPredicate() {

        HashMap<Node, List> familiesHashedByPredicate = new HashMap<>();
        families.forEach(family -> {
            family.getPredicateSet().forEach(predicate -> {
                familiesHashedByPredicate.computeIfAbsent(NodeFactory.createURI(predicate), k -> new ArrayList())
                        .add(family.getIndex());
            });
        });

        return familiesHashedByPredicate;
    }

}
