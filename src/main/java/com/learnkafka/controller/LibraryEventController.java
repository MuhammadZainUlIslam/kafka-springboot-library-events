package com.learnkafka.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
@RequestMapping("/v1")
public class LibraryEventController {

    private final LibraryEventProducer libraryEventProducer;


    public LibraryEventController(LibraryEventProducer libraryEventProducer) {
        this.libraryEventProducer = libraryEventProducer;
    }

    @PostMapping("/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {
    log.info("Library Event :{}",libraryEvent.toString());


/*
    Asynchronic Approach
*/
    libraryEventProducer.sendLibraryEvent(libraryEvent);


        //    Synchronic Approach
//        libraryEventProducer.sendLibraryEvent_approach2(libraryEvent);

        //Approach-3
//        libraryEventProducer.sendLibraryEvent_approach3(libraryEvent);


    log.info("-- After Sending Library Event --");
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


}