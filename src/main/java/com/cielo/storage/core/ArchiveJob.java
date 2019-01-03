package com.cielo.storage.core;

import com.cielo.storage.api.TimeDataUtil;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class ArchiveJob extends QuartzJobBean {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private TimeDataUtil timeDataUtil;
    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        timeDataUtil.archiveJob();
    }
}
