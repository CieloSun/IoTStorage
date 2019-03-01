package com.cielo.storage.model;

import lombok.Data;

@Data
public class DataTag {
    private String tag;
    private DataTag subTag;

    public DataTag() {
    }

    public DataTag(String... tags) {
        DataTag dataTag = this;
        for (int i = 0; i < tags.length; ++i) {
            dataTag.setTag(tags[i]);
            if (i != tags.length - 1) {
                dataTag.setSubTag(new DataTag());
                dataTag = dataTag.getSubTag();
            }
        }
    }

    public DataTag(String dataTagString) {
        this(dataTagString.split("_"));
    }

    @Override
    public String toString() {
        return subTag == null ? tag : tag + "_" + subTag.toString();
    }
}
