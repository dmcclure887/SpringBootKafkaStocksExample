package com.demo.controllers.producers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.logging.Logger;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaTradeProducerController {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    private final String TOPIC = "trade.t";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping(value = "/publishTrade")
    public String sendMessageToKafkaTopic(@RequestParam("message") String message) {
        try {
            kafkaTemplate.send(TOPIC, "sampleKey", message);
            return "*** Sent message: " + message + " to topic: " + TOPIC;
        } catch (Exception e){
            LOG.warning("*** encountered error in method sendMessageToKafkaTopic: " + e.getMessage());
            return "*** encountered error in method sendMessageToKafkaTopic: " + e.getMessage();
        }
    }
}

