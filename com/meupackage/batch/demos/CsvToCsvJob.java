package com.meupackage.batch.demos;

import com.meupackage.batch.demos.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class CsvToCsvJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public FlatFileItemReader<Person> csvReader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                // .resource(new ClassPathResource("input.csv")) // Reads from classpath
                // Or use FileSystemResource for files outside classpath:
                .resource(new FileSystemResource("data/input.csv")) 
                .delimited()
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> personProcessor() {
        return person -> {
            // Example processing: Convert lastName to uppercase
            person.setLastName(person.getLastName().toUpperCase());
            System.out.println("Processing person: " + person.getFirstName() + " " + person.getLastName());
            return person;
        };
    }

    @Bean
    public FlatFileItemWriter<Person> csvWriter() {
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"firstName", "lastName"});

        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        return new FlatFileItemWriterBuilder<Person>()
                .name("personItemWriter")
                .resource(new FileSystemResource("data/output.csv")) // Writes to data/output.csv
                .lineAggregator(lineAggregator)
                .build();
    }

    @Bean
    public Step csvToCsvStep(FlatFileItemReader<Person> csvReader,
                             ItemProcessor<Person, Person> personProcessor,
                             FlatFileItemWriter<Person> csvWriter) {
        return stepBuilderFactory.get("csvToCsvStep")
                .<Person, Person>chunk(10)
                .reader(csvReader)
                .processor(personProcessor)
                .writer(csvWriter)
                .build();
    }

    @Bean
    public Job csvToCsvJob(Step csvToCsvStep) {
        return jobBuilderFactory.get("csvToCsvJob")
                .incrementer(new RunIdIncrementer())
                .flow(csvToCsvStep)
                .end()
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(CsvToCsvJob.class, args);
    }
}
