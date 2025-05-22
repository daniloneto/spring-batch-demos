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
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.validation.BindException;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class FixedWidthJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    // Custom FieldSetMapper
    public static class PersonFieldSetMapper implements FieldSetMapper<Person> {
        @Override
        public Person mapFieldSet(FieldSet fieldSet) throws BindException {
            Person person = new Person();
            // ID is not in the fixed-width file, so we might set a default or leave it
            // person.setId(...); 
            person.setFirstName(fieldSet.readString("firstName").trim());
            person.setLastName(fieldSet.readString("lastName").trim());
            return person;
        }
    }

    @Bean
    public FlatFileItemReader<Person> fixedWidthReader() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
        tokenizer.setNames("firstName", "lastName");
        tokenizer.setColumns(new Range[]{new Range(1, 10), new Range(11, 20)});

        return new FlatFileItemReaderBuilder<Person>()
                .name("personFixedWidthReader")
                .resource(new FileSystemResource("data/input_fixed_width.txt"))
                .lineTokenizer(tokenizer)
                .fieldSetMapper(new PersonFieldSetMapper())
                // Alternatively, use a lambda for simple mapping if Person has an appropriate constructor:
                // .fieldSetMapper(fieldSet -> new Person(fieldSet.readString("firstName").trim(), fieldSet.readString("lastName").trim()))
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> fixedWidthPersonProcessor() {
        return person -> {
            // Example processing: Log and maybe transform
            System.out.println("Processing fixed-width person: " + person.getFirstName() + " " + person.getLastName());
            // No transformation for this demo, just pass through
            return person;
        };
    }

    @Bean
    public ItemWriter<Person> fixedWidthWriter() {
        return items -> {
            System.out.println("Writing items from fixed-width file:");
            for (Person item : items) {
                System.out.println(item.toString());
            }
        };
    }

    @Bean
    public Step fixedWidthStep(FlatFileItemReader<Person> fixedWidthReader,
                               ItemProcessor<Person, Person> fixedWidthPersonProcessor,
                               ItemWriter<Person> fixedWidthWriter) {
        return stepBuilderFactory.get("fixedWidthStep")
                .<Person, Person>chunk(5) // Example chunk size
                .reader(fixedWidthReader)
                .processor(fixedWidthPersonProcessor)
                .writer(fixedWidthWriter)
                .build();
    }

    @Bean
    public Job fixedWidthJob(Step fixedWidthStep) {
        return jobBuilderFactory.get("fixedWidthJob")
                .incrementer(new RunIdIncrementer())
                .flow(fixedWidthStep)
                .end()
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(FixedWidthJob.class, args);
    }
}
