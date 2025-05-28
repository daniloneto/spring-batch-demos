package com.meupackage.batch.demos.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleStepListener implements StepExecutionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleStepListener.class);

    @Override
    public void beforeStep(StepExecution stepExecution) {
        LOGGER.info("Step Starting: {}", stepExecution.getStepName());
        System.out.println("SimpleStepListener: Step Starting - " + stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LOGGER.info("Step Finished: {} with status {}. Read Count: {}, Write Count: {}",
                stepExecution.getStepName(),
                stepExecution.getStatus(),
                stepExecution.getReadCount(),
                stepExecution.getWriteCount());
        System.out.println("SimpleStepListener: Step Finished - " + stepExecution.getStepName() +
                           " with status " + stepExecution.getStatus() +
                           ". Read Count: " + stepExecution.getReadCount() +
                           ", Write Count: " + stepExecution.getWriteCount());
        return stepExecution.getExitStatus();
    }
}
