package com.meupackage.batch.demos;

import com.meupackage.batch.demos.listener.SimpleStepListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class StepListenerJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public ListItemReader<String> listenerDemoReader() {
        List<String> data = Arrays.asList("One", "Two", "Three", "Four", "Five");
        return new ListItemReader<>(data);
    }

    @Bean
    public ItemWriter<String> listenerDemoWriter() {
        return items -> {
            System.out.println("ListenerDemoWriter: Writing items...");
            for (String item : items) {
                System.out.println("  - " + item);
            }
        };
    }

    @Bean
    public SimpleStepListener simpleStepListener() {
        return new SimpleStepListener();
    }

    @Bean
    public Step listenerDemoStep(ListItemReader<String> listenerDemoReader,
                                 ItemWriter<String> listenerDemoWriter,
                                 SimpleStepListener simpleStepListener) {
        return stepBuilderFactory.get("listenerDemoStep")
                .<String, String>chunk(2) // Example chunk size
                .reader(listenerDemoReader)
                .writer(listenerDemoWriter)
                .listener(simpleStepListener) // Attach the listener
                .build();
    }

    @Bean
    public Job stepListenerDemoJob(Step listenerDemoStep) {
        return jobBuilderFactory.get("stepListenerDemoJob")
                .incrementer(new RunIdIncrementer())
                .flow(listenerDemoStep)
                .end()
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(StepListenerJob.class, args);
    }
}
