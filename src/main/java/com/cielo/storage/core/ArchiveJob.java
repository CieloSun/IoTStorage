package com.cielo.storage.core;

import com.cielo.storage.api.TimeDataUtil;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Service;

@Service
public class ArchiveJob extends QuartzJobBean {
    @Autowired
    private TimeDataUtil timeDataUtil;

    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        timeDataUtil.archiveJob();
    }
}
