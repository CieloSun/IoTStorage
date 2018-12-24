package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.api.PersistUtil;
import com.cielo.storage.api.SSDBUtil;
import com.cielo.storage.tool.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.util.List;
import java.util.stream.Collectors;

//用于管理较大value，将数据以地址形式保存到原key减轻负担
@Service
class PersistUtilImpl implements PersistUtil {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSUtil fdfsUtil;
    @Autowired
    private SSDBUtil ssdbUtil;

    //数据先写入SSDB，再异步转移到FDFS中，适用于中型大文件，可以利用SSDB的缓存层减少写入时间
    @Override
    public void persist(String key) throws Exception {
        ssdbUtil.set(key, fdfsUtil.upload(ssdbUtil.get(key).asString()));
    }

    //数据直接写入FDFS，适用于大文件，但FDFS并不适合超过500mb的单个文件存储
    public void setBig(String key, Object val) throws Exception {
        ssdbUtil.set(key, fdfsUtil.upload(SerializationUtils.serialize(val)));
    }

    public Object getBig(String key) throws Exception {
        return SerializationUtils.deserialize(fdfsUtil.downloadBytes(ssdbUtil.get(key).asString()));
    }

    private <T> T getByVal(String val, Class<T> clazz) throws Exception {
        try {
            return JSON.parseObject(val, clazz);
        } catch (JSONException e) {
            return fdfsUtil.downloadObject(val, clazz);
        }
    }

    @Override
    public <T> T get(String key, Class<T> clazz) throws Exception {
        return getByVal(ssdbUtil.get(key).asString(), clazz);
    }

    @Override
    public <T> List<T> getValues(String prefix, Class<T> clazz) throws Exception {
        return ssdbUtil.scan(prefix).values().parallelStream().map(Try.of(value -> getByVal(value, clazz))).collect(Collectors.toList());
    }
}
