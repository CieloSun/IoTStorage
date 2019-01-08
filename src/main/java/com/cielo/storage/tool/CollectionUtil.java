package com.cielo.storage.tool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CollectionUtil {
    public static <T> List<T> toList(Collection<T> collection) {
        if (collection instanceof List) return (List<T>) collection;
        return new ArrayList<T>(collection);
    }
}
