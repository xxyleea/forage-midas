package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private static final Logger log = LoggerFactory.getLogger(TransactionListener.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    public void listen(Transaction transaction, Acknowledgment acknowledgment) {
        log.info("Received transaction: {}", transaction);
        System.out.println("Amount: " + transaction.getAmount());

        // Commit the offset so Kafka knows this message is processed
        acknowledgment.acknowledge();
    }
}