package com.meupackage.batch.demos;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
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
public class SimpleListJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    // ItemReader Bean
    @Bean
    public ItemReader<String> simpleStringReader() {
        List<String> data = Arrays.asList("apple", "banana", "cherry", "date", "elderberry");
        return new ListItemReader<>(data);
    }

    // ItemProcessor Bean
    @Bean
    public ItemProcessor<String, String> simpleStringProcessor() {
        return item -> item.toUpperCase();
    }

    // ItemWriter Bean
    @Bean
    public ItemWriter<String> simpleStringWriter() {
        return items -> {
            for (String item : items) {
                System.out.println("Processed item: " + item);
            }
        };
    }

    // Step Bean
    @Bean
    public Step simpleListStep(ItemReader<String> simpleStringReader,
                               ItemProcessor<String, String> simpleStringProcessor,
                               ItemWriter<String> simpleStringWriter) {
        return stepBuilderFactory.get("simpleListStep")
                .<String, String>chunk(3) // Process items in chunks of 3
                .reader(simpleStringReader)
                .processor(simpleStringProcessor)
                .writer(simpleStringWriter)
                .build();
    }

    // Job Bean
    @Bean
    public Job simpleListJob(Step simpleListStep) {
        return jobBuilderFactory.get("simpleListJob")
                .incrementer(new RunIdIncrementer())
                .flow(simpleListStep)
                .end()
                .build();
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(SimpleListJob.class, args);
    }
}
