package org.apache.ratis.inst.util;

import java.util.*;

public class MathUtil {

    // selects a subset of elements from 0, 1, ... elements, requires numServers >= size
    public static List<Integer> genSubset(Random random, Set<Integer> elements, int size) {
        List<Integer> allElements = new ArrayList<>(elements);
        List<Integer> selected = new ArrayList<>();

        while(selected.size() < size) {
            Integer r = random.nextInt(allElements.size());
            selected.add(allElements.get(r));
            allElements.remove(r);
        }

        return selected;
    }

}
