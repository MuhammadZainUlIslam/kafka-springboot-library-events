package com.learnkafka.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
    public ResponseEntity<LibraryEvent> postLibraryEvent(@Valid  @RequestBody LibraryEvent libraryEvent) throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {
    log.info("Library Event :{}",libraryEvent.toString());

/*
    Asynchronic Approach
*/
/*
    libraryEventProducer.sendLibraryEvent(libraryEvent);
*/


/*            Synchronic Approach
        libraryEventProducer.sendLibraryEvent_approach2(libraryEvent);
        */

//        Approach-3
        libraryEventProducer.sendLibraryEvent_approach3(libraryEvent);


    log.info("-- After Sending Library Event --");
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/libraryevent")
    public ResponseEntity<?> updateLibraryEvent(@Valid  @RequestBody LibraryEvent libraryEvent) throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException {
        log.info("Library Event :{}",libraryEvent);

        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if(BAD_REQUEST != null)
            return BAD_REQUEST;
        libraryEventProducer.sendLibraryEvent_approach3(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(@Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        if(libraryEvent.libraryEventId()== null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please Pass Library Event");
        }
        if(!libraryEvent.libraryEventType().equals(libraryEvent.libraryEventType())){
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please Pass Library Event Id");
        }
        return null;
    }




}