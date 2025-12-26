package org.example.kafka.controller;

import org.example.kafka.model.User;
import org.example.kafka.service.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/users")
public class UserController {

    private final KafkaProducerService producerService;

    public UserController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<String> createUser(@RequestBody User user) {
        user.setId(UUID.randomUUID().toString());
        user.setAction("CREATED");
        producerService.sendMessage(user);
        return ResponseEntity.ok("User creation event sent to Kafka");
    }

    @PutMapping("/{id}")
    public ResponseEntity<String> updateUser(@PathVariable String id, @RequestBody User user) {
        user.setId(id);
        user.setAction("UPDATED");
        producerService.sendMessage(user);
        return ResponseEntity.ok("User update event sent to Kafka");
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> deleteUser(@PathVariable String id) {
        User user = new User(id, null, null, "DELETED");
        producerService.sendMessage(user);
        return ResponseEntity.ok("User deletion event sent to Kafka");
    }
}