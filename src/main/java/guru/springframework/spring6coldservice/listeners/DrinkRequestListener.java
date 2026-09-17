package guru.springframework.spring6coldservice.listeners;

import guru.springframework.spring6coldservice.config.KafkaConfig;
import guru.springframework.spring6coldservice.service.DrinkRequestProcessor;
import guru.springframework.spring6restmvcapi.events.DrinkPreparedEvent;
import guru.springframework.spring6restmvcapi.events.DrinkRequestEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class DrinkRequestListener {

    private final DrinkRequestProcessor drinkRequestProcessor;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(groupId = "ColdListener", topics = KafkaConfig.DRINK_REQUEST_COLD_TOPIC)
    public void listenDrinkRequest(@Payload DrinkRequestEvent drinkRequestEvent) {
        log.debug("I am listening - drink request received");

        drinkRequestProcessor.processDrinkRequest(drinkRequestEvent);

        DrinkPreparedEvent drinkPreparedEvent = DrinkPreparedEvent.builder()
                .beerOrderLineDTO(drinkRequestEvent.getBeerOrderLineDTO())
                .build();

        kafkaTemplate.send(KafkaConfig.DRINK_PREPARED_TOPIC, drinkPreparedEvent);
    }
}
