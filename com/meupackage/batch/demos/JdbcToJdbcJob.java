package com.meupackage.batch.demos;

import com.meupackage.batch.demos.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class JdbcToJdbcJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public DataSource dataSource() {
        // Creates an H2 in-memory database populated with schema.sql and data.sql
        return new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:schema.sql") // Spring Boot will auto-run schema.sql and data.sql if found
                .addScript("classpath:data.sql")   // in src/main/resources by default. Explicitly adding here for clarity.
                .build();
    }

    @Bean
    public JdbcPagingItemReader<Person> jdbcPagingItemReader(DataSource dataSource) {
        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("ID", Order.ASCENDING);

        H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();
        queryProvider.setSelectClause("ID, firstName, lastName");
        queryProvider.setFromClause("FROM PERSON_INPUT");
        queryProvider.setSortKeys(sortKeys);

        return new JdbcPagingItemReaderBuilder<Person>()
                .name("personJdbcPagingReader")
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .pageSize(10) // Example page size
                .rowMapper(new BeanPropertyRowMapper<>(Person.class)) // Maps rows to Person POJO
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> jdbcPersonProcessor() {
        return person -> {
            // Example processing: Convert lastName to uppercase
            System.out.println("Processing person ID: " + person.getId() + " - " + person.getFirstName() + " " + person.getLastName());
            person.setLastName(person.getLastName().toUpperCase());
            return person;
        };
    }

    @Bean
    public JdbcBatchItemWriter<Person> jdbcBatchItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO PERSON_OUTPUT (firstName, lastName) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step jdbcToJdbcStep(JdbcPagingItemReader<Person> jdbcPagingItemReader,
                               ItemProcessor<Person, Person> jdbcPersonProcessor,
                               JdbcBatchItemWriter<Person> jdbcBatchItemWriter) {
        return stepBuilderFactory.get("jdbcToJdbcStep")
                .<Person, Person>chunk(5) // Example chunk size
                .reader(jdbcPagingItemReader)
                .processor(jdbcPersonProcessor)
                .writer(jdbcBatchItemWriter)
                .build();
    }

    @Bean
    public Job jdbcToJdbcJob(Step jdbcToJdbcStep) {
        return jobBuilderFactory.get("jdbcToJdbcJob")
                .incrementer(new RunIdIncrementer())
                .flow(jdbcToJdbcStep)
                .end()
                .build();
    }

    public static void main(String[] args) {
        // Ensure H2 console is not enabled for automated runs if spring.h2.console.enabled=true is in properties
        // System.setProperty("spring.h2.console.enabled","false");
        SpringApplication.run(JdbcToJdbcJob.class, args);
    }
}
