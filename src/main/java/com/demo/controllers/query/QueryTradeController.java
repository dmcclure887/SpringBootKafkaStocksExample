package com.demo.controllers.query;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.logging.Logger;

@RestController
@RequestMapping(value = "/kafka")
public class QueryTradeController {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    @GetMapping(value = "/queryTradeTable")
    public String queryTradeTable() {
        try {
            // TODO add code to query h2 database
            return "database records";
        } catch (Exception e){
            LOG.warning("*** encountered error in method queryTradeTable: " + e.getMessage());
            return "*** encountered error in method queryTradeTable: " + e.getMessage();
        }
    }
}

