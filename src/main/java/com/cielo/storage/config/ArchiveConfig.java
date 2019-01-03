package com.cielo.storage.config;

import com.cielo.storage.core.ArchiveJob;
import com.cielo.storage.core.ClearJob;
import lombok.Data;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties("archive")
public class ArchiveConfig {
    @Value("[\"device\"]")
    private List<String> archiveTags;
    @Value("600000")
    private Long archiveInterval;
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
        return TriggerBuilder.newTrigger().forJob(archiveJobDetail()).withIdentity("archiveJob-trigger").withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(archiveInterval).repeatForever()).build();
    }

    @Bean
    public Trigger clearTrigger() {
        return TriggerBuilder.newTrigger().forJob(clearJobDetail()).withIdentity("clearJob-trigger").withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(clearInterval).repeatForever()).build();
    }
}
