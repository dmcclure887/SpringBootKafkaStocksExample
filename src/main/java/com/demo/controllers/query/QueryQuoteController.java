package com.demo.controllers.query;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.logging.Logger;

@RestController
@RequestMapping(value = "/kafka")
public class QueryQuoteController {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    @GetMapping(value = "/queryQuoteTable")
    public String queryQuoteTable() {
        try {
            // TODO add code to query h2 database
            return "database records";
        } catch (Exception e){
            LOG.warning("*** encountered error in method queryQuoteTable: " + e.getMessage());
            return "*** encountered error in method queryQuoteTable: " + e.getMessage();
        }
    }
}

