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
public class KafkaQuoteGeneratorProducerController {

    private final Logger LOG = Logger.getLogger(this.getClass().getName());

    private boolean startQuoteGenerator = false;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping(value = "/QuoteGenerator")
    public String runQuoteGenerator(@RequestParam(value = "Command: start/stop", required=true)  String command) {
        try {
            if ("START".equals(command.toUpperCase())){
                startQuoteGenerator = true;
                quoteGenerator();
            } else if ("STOP".equals(command.toUpperCase())){
                startQuoteGenerator = false;
            }
        } catch (Exception e) {
            return "*** Encountered error in method runQuoteGenerator: " + e.getMessage();
        }
        LOG.info("*** Successfully executed query");
        return ("*** Successfully executed query");
    }

    public void quoteGenerator() throws InterruptedException {
        List<String> tradeMsgFields = new ArrayList<>();
        tradeMsgFields.add("time");
        tradeMsgFields.add("sym");
        tradeMsgFields.add("bid");
        tradeMsgFields.add("bsize");
        tradeMsgFields.add("ask");
        tradeMsgFields.add("assize");
        tradeMsgFields.add("bex");
        tradeMsgFields.add("aex");

        List<List<String>> symbolsAndExchanges = new ArrayList<>();
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("VOD.L", "150", "156", "XLON"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("HEIN.AS", "100", "105", "XAMS"));
        symbolsAndExchanges.add(buildListOfSymbolExchangeAndPrice("JUVE.MI", "1230", "1240", "XMIC"));

        while (startQuoteGenerator) {
            Thread.sleep(1000);
            List<String> entry = symbolsAndExchanges.get(new Random().nextInt(symbolsAndExchanges.size()));
            int randomBidPrice = getRandomNumberBetweenTwoNumbers(entry.get(1),entry.get(2));
            String msg = buildMsg(tradeMsgFields, entry, randomBidPrice, Calendar.getInstance().toInstant().toString());
            kafkaTemplate.send("quote.t", "quoteKey", msg);
            LOG.info("*** Quote Message: "+ msg);
        }
    }

    private String buildMsg(List<String> tradeMsgFields, List<String> entry, int randomBidPrice, String time) {
        StringBuilder tradeMsg = new StringBuilder();
        tradeMsg.append(tradeMsgFields.get(0) + " : " + time + ", ");
        tradeMsg.append(tradeMsgFields.get(1) + " : " + entry.get(0) + ", ");
        tradeMsg.append(tradeMsgFields.get(2) + " : " + randomBidPrice + ", ");
        tradeMsg.append(tradeMsgFields.get(3) + " : " + getRandomNumberBetweenTwoNumbers("1000","50000") + ", ");
        tradeMsg.append(tradeMsgFields.get(4) + " : " + randomBidPrice++ + ", ");
        tradeMsg.append(tradeMsgFields.get(5) + " : " + getRandomNumberBetweenTwoNumbers("1000","50000") + ", ");
        tradeMsg.append(tradeMsgFields.get(6) + " : " + entry.get(3) + ", ");
        tradeMsg.append(tradeMsgFields.get(7) + " : " + entry.get(3));

        return tradeMsg.toString();
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
