package com.meupackage.batch.tasklets;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.async.SdkPublisher;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Classe responsável por validar arquivos armazenados no S3 de acordo com uma determinada regra de negócio.
 * 
 * Esta classe implementa a interface Tasklet do Spring Batch e pode ser usada para validar arquivos CSV ou qualquer outro do tipo Texto.
 * O processo de validação inclui:
 * - Verificar a presença de um conteúdo
 * - Utilizar o Amazon S3 para buscar e processar o arquivo
 * 
 * Em caso de erro ou ausência desse, o status de saída do StepContribution é ajustado para "FAILED".
 * Esse processo é extremamente mais rápido do que baixar o arquivo e fazer a validação local.
 * 
 * 
 * Class responsible for validating files stored in S3 according to a specific business rule.
 * 
 * This class implements the Tasklet interface from Spring Batch and can be used to validate CSV files or any other text files.
 * The validation process includes:
 * - Checking for the presence of content
 * - Using Amazon S3 to fetch and process the file
 * 
 * In case of an error or absence of content, the exit status of the StepContribution is set to "FAILED".
 * This process is significantly faster than downloading the file and performing the validation locally.
 */
@Component
public class ValidateFile implements Tasklet {    

    private final S3Client client;
    private final String bucket;
    private final String fileName;

    public ValidateFile(S3Client client, String bucket, String fileName) {
        this.client = client;
        this.bucket = bucket;
        this.fileName = fileName;
    }

@Override
public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {    
  
    SelectObjectContentRequest selectRequest = createSelectRequest(fileName);

    try (S3AsyncClient s3AsyncClient = S3AsyncClient.builder().build()) {
        CompletableFuture<Void> future = s3AsyncClient.selectObjectContent(selectRequest, new FileValidationResponseHandler(contribution));

        try {
            future.get(); // Wait async
        } catch (InterruptedException | ExecutionException e) {
            handleValidationError(contribution, "Error validating file.", e);
        }
    }
  
    return RepeatStatus.FINISHED;
}

private SelectObjectContentRequest createSelectRequest(String fileName) {
    String sqlExpression = "SELECT _1 FROM S3Object WHERE _1 LIKE '%XPTO'";

    return SelectObjectContentRequest.builder()
            .bucket(bucket)
            .key(fileName)
            .expressionType(ExpressionType.SQL)
            .expression(sqlExpression)
            .inputSerialization(InputSerialization.builder()
                    .csv(CSVInput.builder().fileHeaderInfo(FileHeaderInfo.NONE).build())
                    .compressionType(CompressionType.NONE)
                    .build())
            .outputSerialization(OutputSerialization.builder()
                    .csv(CSVOutput.builder().build())
                    .build())
            .build();
}

private void handleValidationError(StepContribution contribution, String message, Exception e) {    
    contribution.setExitStatus(new ExitStatus("FAILED", message));
}

private class FileValidationResponseHandler implements SelectObjectContentResponseHandler {
    private final StepContribution contribution;
    private boolean hasXPTO = false;    

    public FileValidationResponseHandler(StepContribution contribution) {
        this.contribution = contribution;
    }

    @Override
    public void responseReceived(SelectObjectContentResponse response) {
        //
    }

    @Override
    public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
        publisher.subscribe(event -> {
            if (event instanceof RecordsEvent) {
                String records = StandardCharsets.UTF_8.decode(((RecordsEvent) event).payload().asByteBuffer()).toString();
                for (String line : records.split("\n")) {
                    if (line.endsWith("XPTO")) {
                        hasXPTO = true;
                    }                  
                }
            }
        });
    }

    @Override
    public void complete() {       
        if (!hasXPTO) {            
            contribution.setExitStatus(new ExitStatus("FAILED", "Invalid file, XPTO not found."));
        }
    }

    @Override
    public void exceptionOccurred(Throwable throwable) {        
        contribution.setExitStatus(new ExitStatus("FAILED", "S3 error."));
    }
}

}
