package com.meupackage.batch.demos;

import com.meupackage.batch.demos.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class MultiResourceJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    // Delegate Reader - No resource set here
    @Bean
    public FlatFileItemReader<Person> csvFileDelegateReader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("csvFileDelegateReader")
                // .resource() // Resource is set by MultiResourceItemReader
                .delimited()
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public MultiResourceItemReader<Person> multiResourceItemReader() throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("file:data/input_multi_*.csv");

        return new MultiResourceItemReaderBuilder<Person>()
                .name("multiPersonReader")
                .resources(resources)
                .delegate(csvFileDelegateReader())
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> multiResourcePersonProcessor() {
        return person -> {
            System.out.println("Processing person from multi-resource: " + person.getFirstName() + " " + person.getLastName());
            // Example: Convert firstName to lowercase for differentiation
            person.setFirstName(person.getFirstName().toLowerCase());
            return person;
        };
    }

    @Bean
    public ItemWriter<Person> multiResourceWriter() {
        return items -> {
            System.out.println("Writing items from multiple resources:");
            for (Person item : items) {
                System.out.println(item.toString());
            }
        };
    }

    @Bean
    public Step multiResourceStep(MultiResourceItemReader<Person> multiResourceItemReader,
                                  ItemProcessor<Person, Person> multiResourcePersonProcessor,
                                  ItemWriter<Person> multiResourceWriter) {
        return stepBuilderFactory.get("multiResourceStep")
                .<Person, Person>chunk(3) // Example chunk size
                .reader(multiResourceItemReader)
                .processor(multiResourcePersonProcessor)
                .writer(multiResourceWriter)
                .build();
    }

    @Bean
    public Job multiResourceJob(Step multiResourceStep) {
        return jobBuilderFactory.get("multiResourceJob")
                .incrementer(new RunIdIncrementer())
                .flow(multiResourceStep)
                .end()
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(MultiResourceJob.class, args);
    }
}
