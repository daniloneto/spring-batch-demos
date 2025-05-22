package com.meupackage.batch.demos;

import com.meupackage.batch.demos.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class JobParametersDemoJob implements CommandLineRunner {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobLauncher jobLauncher;

    // Autowire the Job bean itself to be used in CommandLineRunner
    @Autowired
    private Job resolvedJobParametersDemoJob;

    @Bean
    @StepScope // Crucial for late binding of job parameters
    public FlatFileItemReader<Person> jobParamsCsvReader(@Value("#{jobParameters['inputFile']}") Resource inputFile) {
        if (inputFile == null) {
            // This case should ideally not be hit if a default is always provided for the job parameter.
            throw new IllegalStateException("inputFile parameter is null. It must be provided.");
        }
        if (!inputFile.exists()) {
            throw new IllegalStateException("Input file does not exist: " + inputFile);
        }
        return new FlatFileItemReaderBuilder<Person>()
                .name("jobParamsPersonReader")
                .resource(inputFile)
                .delimited()
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> jobParamsPersonProcessor() {
        return person -> {
            System.out.println("Processing (job params demo): " + person.getFirstName() + " " + person.getLastName());
            // No transformation, just pass through
            return person;
        };
    }

    @Bean
    public ItemWriter<Person> jobParamsWriter() {
        return items -> {
            System.out.println("Writing items (job params demo):");
            for (Person item : items) {
                System.out.println(item.toString());
            }
        };
    }

    @Bean
    public Step jobParametersDemoStep(FlatFileItemReader<Person> jobParamsCsvReader,
                                      ItemProcessor<Person, Person> jobParamsPersonProcessor,
                                      ItemWriter<Person> jobParamsWriter) {
        return stepBuilderFactory.get("jobParametersDemoStep")
                .<Person, Person>chunk(5)
                .reader(jobParamsCsvReader)
                .processor(jobParamsPersonProcessor)
                .writer(jobParamsWriter)
                .build();
    }

    // This bean definition will be autowired into 'resolvedJobParametersDemoJob'
    @Bean
    public Job jobParametersDemoJob(Step jobParametersDemoStep) {
        return jobBuilderFactory.get("jobParametersDemoJob")
                .incrementer(new RunIdIncrementer()) 
                .flow(jobParametersDemoStep)
                .end()
                .build();
    }

    public static void main(String... args) { // Consistent varargs signature
        SpringApplication.run(JobParametersDemoJob.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Launching JobParametersDemoJob via CommandLineRunner...");
        JobParametersBuilder paramsBuilder = new JobParametersBuilder();

        // Add a timestamp to ensure job instance uniqueness for reruns
        paramsBuilder.addLong("timestamp", System.currentTimeMillis());

        // Check for command line arguments to override inputFile
        String inputFileValue = null;
        for (String arg : args) {
            if (arg.startsWith("inputFile=")) {
                inputFileValue = arg.substring("inputFile=".length());
                break;
            }
        }

        if (inputFileValue != null && !inputFileValue.isEmpty()) {
            paramsBuilder.addString("inputFile", inputFileValue);
            System.out.println("Using inputFile from command line: " + inputFileValue);
        } else {
            String defaultInputFile = "file:data/input.csv"; // Default input file
            paramsBuilder.addString("inputFile", defaultInputFile);
            System.out.println("Using default inputFile: " + defaultInputFile);
        }
        
        JobParameters jobParameters = paramsBuilder.toJobParameters();
        
        // Launch the autowired job bean
        jobLauncher.run(resolvedJobParametersDemoJob, jobParameters);
        System.out.println("JobParametersDemoJob finished.");
    }
}
