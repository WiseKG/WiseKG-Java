package org.linkeddatafragments.characteristicset;

import org.linkeddatafragments.util.StarString;

import java.util.Set;

public interface ICharacteristicSet {
    boolean matches(StarString starPattern);
    double count(StarString starPattern);
    double getDistinct();
    double count(StarString starPattern, Set<String> vars, long numBindings);
}
