package com.cielo.storage.api;

import java.util.List;

public interface PersistUtil {
    void persist(String key) throws Exception;

    public void setBig(String key, Object val) throws Exception;

    <T> T get(String key, Class<T> clazz) throws Exception;

    public Object getBig(String key) throws Exception;

    <T> List<T> getValues(String prefix, Class<T> clazz) throws Exception;

    void del(String key) throws Exception;
}
