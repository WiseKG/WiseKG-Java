package org.wisekg.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;


public class FamiliesHdtCache {

    private static final FamiliesHdtCache familiesHdtCache = new FamiliesHdtCache();
    private final LoadingCache<Integer, HDT> cache;
    //Stopwatch stopwatch = Stopwatch.createStarted();

    private FamiliesHdtCache() {


        cache = CacheBuilder.newBuilder()
                .maximumSize(20000) //Maximum caching size
                .build(new CacheLoader<Integer, HDT>() {
                    @Override
                    public HDT load(Integer k) throws Exception {
                        // System.out.println("Load");
                        return addcache(k);
                    }

                });



    }

    public void releaseCache() {

        System.out.println("Cache Size:" + cache.size());
        cache.asMap().entrySet().forEach((m) -> {
            try {
                System.out.println("releaseCache");
                //Object key = m.getKey();
                m.getValue().close();
            } catch (IOException ex) {
                Logger.getLogger(FamiliesHdtCache.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        System.out.println("Cache Size:" + cache.size());
        cache.invalidateAll();

        //familiesHdtCache = null;
        // familiesHdtCache = new FamiliesHdtCache();
    }

    public static FamiliesHdtCache getInstance() {
        return familiesHdtCache;
    }

    private HDT addcache(Integer family) {
        // System.out.print("adding chache");
        HDT hdt = null;
        try {
            Family f = FamiliesConfig.getInstance().getFamilyByID(family);

            //   System.out.println("Hdt File" + f.getName());
            String filePathString = Config.getInstance().getDownloadedpartitions() + f.getName();
            String fileindexPathString = Config.getInstance().getDownloadedpartitions() + f.getName() + ".index.v1-1";
            File hdtfile = new File(filePathString);
            File indexfile = new File(fileindexPathString);

            hdt = HDTManager.mapIndexedHDT(filePathString, null);
        } catch (IOException ex) {
            Logger.getLogger(FamiliesHdtCache.class.getName()).log(Level.SEVERE, null, ex);
        }
        return hdt;
    }

    public HDT getEntry(Integer i) throws ExecutionException {
        return cache.get(i);
    }
}
