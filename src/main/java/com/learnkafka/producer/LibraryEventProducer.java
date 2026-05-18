package com.learnkafka.producer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    private final ObjectMapper objectMapper;

    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }


    //    Synchronic Approach
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var completableFuture = kafkaTemplate.send(topic, key, value);

        return completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);

                    } else {

                        handleSucess(key, value, sendResult);
                    }
                });
    }

    private void handleSucess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully for key {} and value {} partition is {}", key, value, sendResult.getProducerRecord().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {

        log.error("Error sending the message and the exception is {}", throwable.getMessage(), throwable);
    }


    //    Synchronic Approach
    public SendResult<Integer, String> sendLibraryEvent_approach2(LibraryEvent libraryEvent)
            throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        var sendResult = kafkaTemplate.send(topic, key, value)
//                .get();
                .get(3, TimeUnit.SECONDS);
        handleSucess(key, value, sendResult);
        return sendResult;
    }




    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
         var producerRecord = buildProducerRecord(key,value);

        var completableFuture = kafkaTemplate.send(producerRecord);
        return completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);

                    } else {

                        handleSucess(key, value, sendResult);
                    }
                });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
       List<Header> RecordHeaders=List.of(new RecordHeader("event-source","scanner".getBytes()));

        return new ProducerRecord<>(topic,null,key,value, RecordHeaders);
    }
}
