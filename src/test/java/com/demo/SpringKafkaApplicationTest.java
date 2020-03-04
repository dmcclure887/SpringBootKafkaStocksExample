package com.demo;

import com.demo.consumer.KafkaQuoteConsumer;
import com.demo.consumer.KafkaTradeConsumer;
import com.demo.controllers.producers.KafkaQuoteProducerController;
import com.demo.controllers.producers.KafkaTradeProducerController;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
// keep this commented out to test with local instance of kafka
//@EmbeddedKafka(partitions = 1, topics = {SpringKafkaApplicationTest.HELLOWORLD_TOPIC})
public class SpringKafkaApplicationTest {

  @Autowired
  private KafkaQuoteConsumer quoteConsumer;

  @Autowired
  private KafkaQuoteProducerController quoteProducer;

  @Autowired
  private KafkaTradeConsumer tradeConsumer;

  @Autowired
  private KafkaTradeProducerController tradeProducer;

  @Test
  public void testQuote() throws Exception {
      quoteProducer.sendMessageToKafkaTopic("Hello Spring Kafka!");
      Assert.assertEquals("*** Received message from topic quote.t: ", quoteConsumer.listen(Matchers.anyString()));
  }

    @Test
    public void testTrade() throws Exception {
        tradeProducer.sendMessageToKafkaTopic("Hello Spring Kafka!");
        Assert.assertEquals("*** Received message from topic trade.t: ", tradeConsumer.listen(Matchers.anyString()));
    }
}
