package com.learnkafka.controller;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.*;


import static org.junit.jupiter.api.Assertions.*;



@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LibraryEventControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;


    @Test
    void postLibraryEvent() {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(),  httpHeaders);

        var responseEntity = restTemplate
                .exchange("/v1/libraryevent", HttpMethod.POST,httpEntity, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }
}