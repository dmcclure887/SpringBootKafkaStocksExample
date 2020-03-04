package com.demo.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.logging.Logger;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaTradeConsumer {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    //@GetMapping(value = "/consumeTrade")
    @KafkaListener(topics = "trade.t")
    public String listen(@Payload String message) throws IOException, InterruptedException {
        LOG.info("*** Received message from topic trade.t: " + message);
        return         "*** Received message from topic trade.t: " + message;
    }


}
