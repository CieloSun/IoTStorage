package com.cielo.storage.config;

import com.cielo.storage.core.ArchiveJob;
import lombok.Data;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Data
@Component
@ConfigurationProperties("time-data")
public class TimeDataConfig {
    @Value("[\"device\"]")
    private List<String> archiveTags;
    //归档间隔默认为30分钟
    @Value("1800")
    private Integer archiveInterval;
    //清理间隔默认为72小时
    @Value("259200")
    private Long clearInterval;
    @Value("0")
    private Integer leastClearNum;
    @Value("true")
    private Boolean deleteValueTogether;
    @Value("true")
    private Boolean saveKeyInValue;

    @Bean
    public JobDetail archiveJobDetail() {
        return JobBuilder.newJob(ArchiveJob.class).withIdentity("archiveJob").storeDurably().build();
    }

    @Bean
    public Trigger archiveTrigger() {
        return TriggerBuilder.newTrigger().forJob(archiveJobDetail()).withIdentity("archiveJob-trigger")
                .startAt(new Date(System.currentTimeMillis() + archiveInterval * 1000l))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(archiveInterval)
                        .repeatForever()).build();
    }
}
