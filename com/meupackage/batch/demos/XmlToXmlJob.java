package com.meupackage.batch.demos;

import com.meupackage.batch.demos.model.Person;
import com.thoughtworks.xstream.security.AnyTypePermission;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class XmlToXmlJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public XStreamMarshaller personMarshaller() {
        XStreamMarshaller marshaller = new XStreamMarshaller();
        Map<String, Class<?>> aliases = new HashMap<>();
        aliases.put("person", Person.class);
        marshaller.setAliases(aliases);
        // For security, explicitly allow types to be processed
        marshaller.setTypePermissions(AnyTypePermission.ANY);
        // Or more restrictively:
        // marshaller.setSupportedClasses(Person.class);
        return marshaller;
    }

    @Bean
    public StaxEventItemReader<Person> xmlReader(XStreamMarshaller personMarshaller) {
        return new StaxEventItemReaderBuilder<Person>()
                .name("personXmlItemReader")
                .resource(new FileSystemResource("data/input.xml"))
                .addFragmentRootElements("person")
                .unmarshaller(personMarshaller)
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> xmlPersonProcessor() {
        return person -> {
            // Example processing: Convert firstName to uppercase
            System.out.println("Processing person: " + person.getFirstName() + " " + person.getLastName());
            person.setFirstName(person.getFirstName().toUpperCase());
            return person;
        };
    }

    @Bean
    public StaxEventItemWriter<Person> xmlWriter(XStreamMarshaller personMarshaller) {
        return new StaxEventItemWriterBuilder<Person>()
                .name("personXmlItemWriter")
                .resource(new FileSystemResource("data/output.xml"))
                .marshaller(personMarshaller)
                .rootTagName("persons")
                .overwriteOutput(true)
                .build();
    }

    @Bean
    public Step xmlToXmlStep(StaxEventItemReader<Person> xmlReader,
                             ItemProcessor<Person, Person> xmlPersonProcessor,
                             StaxEventItemWriter<Person> xmlWriter) {
        return stepBuilderFactory.get("xmlToXmlStep")
                .<Person, Person>chunk(10)
                .reader(xmlReader)
                .processor(xmlPersonProcessor)
                .writer(xmlWriter)
                .build();
    }

    @Bean
    public Job xmlToXmlJob(Step xmlToXmlStep) {
        return jobBuilderFactory.get("xmlToXmlJob")
                .incrementer(new RunIdIncrementer())
                .flow(xmlToXmlStep)
                .end()
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(XmlToXmlJob.class, args);
    }
}
