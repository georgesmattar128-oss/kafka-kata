package com.learn.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class ProducerController {

    @Autowired
    private MessageProducer messageProducer;

    @PostMapping("/produce")
    public String produceMessage(@RequestParam String content) {
        messageProducer.sendMessage(content);
        return "Message envoyé : " + content;
    }
	@GetMapping("/test")
public String test() {
    return "ok";
}
}