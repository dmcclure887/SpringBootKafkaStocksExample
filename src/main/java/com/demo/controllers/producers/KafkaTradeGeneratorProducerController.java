package com.demo.controllers.producers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaTradeGeneratorProducerController {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    private boolean startTradeGenerator = false;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping(value = "/TradeGenerator")
    public String runTradeGenerator(@RequestParam(value = "Command: start/stop", required=true)  String command) {
        try {
            if ("START".equals(command.toUpperCase())){
                startTradeGenerator = true;
                tradeGenerator();
            } else if ("STOP".equals(command.toUpperCase())){
                startTradeGenerator = false;
            }
        } catch (Exception e) {
            return "*** Encountered error in method runTradeGenerator: " + e.getMessage();
        }
        LOG.info("*** Successfully executed query");
        return ("*** Successfully executed query");
    }

    public void tradeGenerator() throws InterruptedException {
        List<String> tradeMsgFields = new ArrayList<>();
        tradeMsgFields.add("time");
        tradeMsgFields.add("sym");
        tradeMsgFields.add("price");
        tradeMsgFields.add("size");
        tradeMsgFields.add("ex");

        List<List<String>> symbolsAndExchanges = new ArrayList<>();
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("VOD.L", "150", "156", "XLON"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("HEIN.AS", "100", "105", "XAMS"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("JUVE.MI", "1230", "1240", "XMIC"));

        while (startTradeGenerator) {
            Thread.sleep(1000);
            List<String> entry = symbolsAndExchanges.get(new Random().nextInt(symbolsAndExchanges.size()));
            String msg = buildMsg(tradeMsgFields, entry, Calendar.getInstance().toInstant().toString());
            kafkaTemplate.send("trade.t", "tradeKey", msg);
            LOG.info("*** Trade Message: " + msg);
        }
    }

    private String buildMsg(List<String> tradeMsgFields, List<String> entry, String time) {
        StringBuilder quoteMsg = new StringBuilder();
        quoteMsg.append(tradeMsgFields.get(0) + " : " + time + ", ");
        quoteMsg.append(tradeMsgFields.get(1) + " : " + entry.get(0) + ", ");
        quoteMsg.append(tradeMsgFields.get(2) + " : " + getRandomNumberBetweenTwoNumbers(entry.get(1),entry.get(2)) + ", ");
        quoteMsg.append(tradeMsgFields.get(3) + " : " + getRandomNumberBetweenTwoNumbers("1000","50000") + ", ");
        quoteMsg.append(tradeMsgFields.get(4) + " : " + entry.get(3));

        return quoteMsg.toString();
    }

    private static List<String> buildListOfSymbolExchangeAndPrice(String symbol, String low, String high, String exchange) {
        List<String> items = new ArrayList<>();
        items.add(symbol);
        items.add(low);
        items.add(high);
        items.add(exchange);

        return items;
    }

    private static int getRandomNumberBetweenTwoNumbers(String low, String high) {
        Random r = new Random();
        return r.nextInt(Integer.parseInt(high)-Integer.parseInt(low)) + Integer.parseInt(low);
    }

}
