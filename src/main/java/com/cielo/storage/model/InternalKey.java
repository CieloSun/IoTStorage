package com.cielo.storage.model;

import java.util.HashMap;
import java.util.Map;

public class InternalKey {
    public static final String TAG_PREFIX = "tag@@";
    public static final String NO_TYPE = "nT@@";
    private String tag;
    private String type;
    private InternalKey subTag;

    private InternalKey() {
    }

    public InternalKey(String... tagsInStr) {
        InternalKey internalKey = this;
        for (int i = 0; i < tagsInStr.length; ++i) {
            String tagInStr = tagsInStr[i];
            if (tagInStr.contains("/")) {
                String[] splitTagInStr = tagInStr.split("/", 2);
                internalKey.type = splitTagInStr[0];
                internalKey.tag = splitTagInStr[1];
            } else {
                type = NO_TYPE;
                tag = tagInStr;
            }
            if (i != tagsInStr.length - 1) {
                internalKey.subTag = new InternalKey();
                internalKey = internalKey.subTag;
            }
        }
    }

    public InternalKey(String internalKeyString) {
        this(internalKeyString.split("_"));
    }

    public Map<String, String> typeTagPairs() {
        Map<String, String> map = new HashMap<>();
        InternalKey internalKey = this;
        while (internalKey != null) {
            map.put(internalKey.type, internalKey.tag);
            internalKey = internalKey.subTag;
        }
        return map;
    }

    @Override
    public String toString() {
        return subTag == null ? type + "/" + tag : type + "/" + tag + "_" + subTag.toString();
    }
}
