package com.cielo.storage.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DataTag {
    private String tag;
    private DataTag subTag;

    public DataTag() {
    }

    public DataTag(String... tags) {
        tag = tags[0];
        if (tags.length == 2) subTag = new DataTag(tags[1]);
    }

    public DataTag(String dataTagString) {
        String[] tags = dataTagString.split("_", 2);
        tag = tags[0];
        if (tags.length == 2) subTag = new DataTag(tags[1]);
    }

    @Override
    public String toString() {
        return subTag == null ? tag : tag + "_" + subTag.toString();
    }

    public List<String> tags() {
        List list = new ArrayList();
        DataTag dataTag = this;
        list.add(dataTag.tag);
        while (dataTag.subTag != null) {
            dataTag = dataTag.getSubTag();
            list.add(dataTag.getTag());
        }
        return list;
    }
}
