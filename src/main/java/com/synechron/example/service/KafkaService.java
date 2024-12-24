package com.synechron.example.service;

import com.synechron.example.model.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

@Service
class KafkaService {

    @Value("${even.topic}")
    private String evenTopic;

    @Value("${odd.topic}")
    private String oddTopic;

   @Autowired
   KafkaTemplate<String, Person> kafkaTemplate;
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public KafkaService(KafkaTemplate<String, Person> kafkaTemplate, JdbcTemplate jdbcTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(topics = "${inbound.topic}", groupId = "group_id")
    public void processMessage(ConsumerRecord<String, Person> record) {
        Person person = record.value();
        int age = calculateAge(person.getDateOfBirth());

        String targetTopic = (age % 2 == 0) ? evenTopic : oddTopic;
        ListenableFuture<SendResult<String, Person>> future = (ListenableFuture<SendResult<String, Person>>) kafkaTemplate.send(new ProducerRecord<>(targetTopic, person));

        future.addCallback(
                success -> {
                    persistToDatabase(person, "EVEN_TOPIC");
                    System.out.println("Message published successfully to " + targetTopic);
                },
                failure -> System.err.println("Failed to publish message to " + targetTopic)
        );
    }

    private int calculateAge(String dateOfBirth) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate dob = LocalDate.parse(dateOfBirth, formatter);
        return Period.between(dob, LocalDate.now()).getYears();
    }

    private void persistToDatabase(Person payload, String topic) {
        String sql = "INSERT INTO published_messages (name, address, date_of_birth, topic) VALUES (?, ?, ?, ?)";
        jdbcTemplate.update(sql, payload.getName(), payload.getAddress(), payload.getDateOfBirth(), topic);
    }
}