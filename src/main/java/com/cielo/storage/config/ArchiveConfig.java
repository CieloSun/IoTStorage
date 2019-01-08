package com.cielo.storage.config;

import com.cielo.storage.core.ArchiveJob;
import com.cielo.storage.core.ClearJob;
import lombok.Data;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

@Data
@Component
@ConfigurationProperties("archive")
public class ArchiveConfig {
    @Value("[\"device\"]")
    private List<String> archiveTags;
    //归档间隔默认为30分钟
    @Value("1800000")
    private Long archiveInterval;
    //清理间隔默认为24小时
    @Value("86400000")
    private Long clearInterval;
    @Value("0")
    private Integer leastClearNum;

    @Bean
    public JobDetail archiveJobDetail() {
        return JobBuilder.newJob(ArchiveJob.class).withIdentity("archiveJob").storeDurably().build();
    }


    @Bean
    public JobDetail clearJobDetail() {
        return JobBuilder.newJob(ClearJob.class).withIdentity("clearJob").storeDurably().build();
    }

    @Bean
    public Trigger archiveTrigger() {
        return TriggerBuilder.newTrigger().forJob(archiveJobDetail()).withIdentity("archiveJob-trigger")
                .startAt(new Date(System.currentTimeMillis() + archiveInterval))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(archiveInterval)
                        .repeatForever()).build();
    }

    @Bean
    public Trigger clearTrigger() {
        return TriggerBuilder.newTrigger().forJob(clearJobDetail()).withIdentity("clearJob-trigger")
                .startAt(new Date(System.currentTimeMillis() + clearInterval))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(clearInterval)
                        .repeatForever()).build();
    }
}
