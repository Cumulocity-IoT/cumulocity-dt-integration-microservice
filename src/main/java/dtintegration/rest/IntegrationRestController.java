package dtintegration.rest;

import dtintegration.services.IntegrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IntegrationRestController {

    @Autowired
    IntegrationService integrationService;

    final Logger logger = LoggerFactory.getLogger(IntegrationRestController.class);


    @RequestMapping(value = "/receiver", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity dtReceiver(@RequestBody String event) {
        logger.info("Event received  {}", event);
        integrationService.createLoggingEvent(event);
        integrationService.mapEventToC8Y(event);
        return ResponseEntity.ok().build();
    }
}
