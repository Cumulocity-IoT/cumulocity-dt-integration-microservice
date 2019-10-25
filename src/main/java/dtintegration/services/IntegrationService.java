package dtintegration.services;

import c8y.*;
import com.cumulocity.microservice.subscription.service.MicroserviceSubscriptionsService;
import com.cumulocity.model.DateTimeConverter;
import com.cumulocity.model.ID;
import com.cumulocity.model.measurement.MeasurementValue;
import com.cumulocity.rest.representation.event.EventRepresentation;
import com.cumulocity.rest.representation.identity.ExternalIDRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.sdk.client.SDKException;
import com.cumulocity.sdk.client.event.EventApi;
import com.cumulocity.sdk.client.event.EventCollection;
import com.cumulocity.sdk.client.event.EventFilter;
import com.cumulocity.sdk.client.identity.IdentityApi;
import com.cumulocity.sdk.client.inventory.InventoryApi;
import com.cumulocity.sdk.client.measurement.MeasurementApi;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dtintegration.rest.IntegrationRestController;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Service
public class IntegrationService {
    @Autowired
    EventApi eventApi;

    @Autowired
    InventoryApi inventoryApi;

    @Autowired
    IdentityApi identityApi;

    @Autowired
    MeasurementApi measurementApi;

    @Autowired
    MicroserviceSubscriptionsService subscriptionsService;

    private final String LOGGING_ID = "DI_INTEGRATION_LOGGING";
    private final String SERIAL_TYPE = "c8y_Serial";

    private ManagedObjectRepresentation loggingDevice;

    final Logger logger = LoggerFactory.getLogger(IntegrationRestController.class);

    public void mapEventToC8Y(String eventString) {
        subscriptionsService.runForEachTenant(() -> {


            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode rootNode = mapper.readTree(eventString);
                JsonNode eventNode = rootNode.get("event");
                JsonNode labelNode = rootNode.get("labels");
                String targetName = eventNode.get("targetName").asText();
                DateTime updateTime = DateTimeConverter.string2Date(eventNode.get("timestamp").asText());
                String eventType = eventNode.get("eventType").asText();
                JsonNode dataNode = eventNode.get("data");
                String deviceId = null;
                String deviceName = null;
                if (targetName != null) {
                    String[] targets = targetName.split("/");
                    deviceId = targets[targets.length - 1];
                }
                if (labelNode != null) {
                    deviceName = labelNode.get("name").asText();
                }
                if (deviceId != null && deviceName != null)
                    upsertDTDevice(deviceName, deviceId, dataNode, updateTime);
            } catch (Exception e) {
                logger.error("Error on Mapping DT-Event to Cumulocity", e);
            }
        });
    }

    public void createLoggingEvent(String loggingPayload) {
        subscriptionsService.runForEachTenant(() -> {
            ManagedObjectRepresentation loggingMor = findLoggingDevice();
            if (loggingMor != null) {
                EventRepresentation representation = new EventRepresentation();
                representation.setType("dt_Event");
                representation.setDateTime(DateTime.now());
                representation.setSource(loggingMor);
                representation.setText("DT Logging Event");
                representation.set(loggingPayload, "dt_EventPayload");
                eventApi.create(representation);
            }
        });

    }

    private ManagedObjectRepresentation findLoggingDevice() {
        if (loggingDevice == null) {
            ExternalIDRepresentation extId = findExternalId(LOGGING_ID, SERIAL_TYPE);
            if (extId == null) {
                return createLoggingDevice();
            } else
                return extId.getManagedObject();
        } else
            return loggingDevice;
    }

    public ExternalIDRepresentation findExternalId(String externalId, String type) {
        ID id = new ID();
        id.setType(type);
        id.setValue(externalId);
        ExternalIDRepresentation extId = null;
        try {
            extId = identityApi.getExternalId(id);
        } catch (SDKException e) {
            logger.info("External ID {} not found", externalId);
        }
        return extId;
    }

    private ManagedObjectRepresentation createLoggingDevice() {
        try {
            ManagedObjectRepresentation mor = new ManagedObjectRepresentation();
            mor.setType("c8y_LoggingDevice");
            mor.setName("DT Logging Device");
            mor = inventoryApi.create(mor);
            ExternalIDRepresentation extId = new ExternalIDRepresentation();
            extId.setExternalId(LOGGING_ID);
            extId.setType(SERIAL_TYPE);
            extId.setManagedObject(mor);
            identityApi.create(extId);
            return mor;
        } catch (SDKException e) {
            logger.info("Error on creating Logging Device", e);
            return null;
        }
    }

    private ManagedObjectRepresentation upsertDTDevice(String name, String id, JsonNode data, DateTime updateTime) {
        try {
            logger.info("Upsert device with name {} and id {} with Data {}", name, id, data);
            ExternalIDRepresentation extId = findExternalId(id, SERIAL_TYPE);
            ManagedObjectRepresentation mor;
            boolean deviceExists = true;
            if (extId == null) {
                mor = new ManagedObjectRepresentation();
                mor.setType("c8y_DTDevice");
                mor.setName(name);
                deviceExists = false;
            } else {
                mor = extId.getManagedObject();
            }
            mor.set(new IsDevice());

            if (data.has("networkStatus")) {
                mor.set(data.get("networkStatus").get("signalStrength").asInt(), "signalStrength");
                createSignalStrengthMeasurement(mor, data.get("networkStatus").get("signalStrength").asInt(), updateTime);
            }
            if (data.has("temperature")) {
                mor.set(data.get("temperature").get("value").asDouble(), "temperature");
                createTemperatureMeasurement(mor, data.get("temperature").get("value").asDouble(), updateTime);
            }
            if (data.has("objectPresent")) {
                String eventText = null;
                boolean isOpen = false;
                if ("PRESENT".equals(data.get("objectPresent").get("state").asText())) {
                    isOpen = false;
                    eventText = "Object closed";
                } else {
                    isOpen = true;
                    eventText = "Object opened";
                }
                mor.set(isOpen, "isOpen");
                createEvent(mor, "c8y_OpenCloseEvent", eventText, isOpen, updateTime);
            }

            if(data.has("batteryStatus")) {
                mor.set(data.get("batteryStatus").get("percentage").asDouble(), "battery");
                createBatteryMeasurement(mor,data.get("batteryStatus").get("percentage").asDouble(), updateTime );
            }
            mor.set(DateTimeConverter.date2String(updateTime), "lastDTUpdate");
            if (!deviceExists) {
                mor = inventoryApi.create(mor);
                extId = new ExternalIDRepresentation();
                extId.setExternalId(id);
                extId.setType(SERIAL_TYPE);
                extId.setManagedObject(mor);
                identityApi.create(extId);
            } else
                mor = inventoryApi.update(mor);
            return mor;
        } catch (SDKException e) {
            logger.info("Error on creating DT Device", e);
            return null;
        }
    }

    public void createTemperatureMeasurement(ManagedObjectRepresentation mor, Double temperature, DateTime dateTime) {
        try {
            MeasurementRepresentation measurementRepresentation = new MeasurementRepresentation();
            TemperatureMeasurement temperatureMeasurement = new TemperatureMeasurement();
            temperatureMeasurement.setTemperature(BigDecimal.valueOf(temperature));
            measurementRepresentation.set(temperatureMeasurement);
            measurementRepresentation.setSource(mor);
            measurementRepresentation.setDateTime(dateTime);
            measurementRepresentation.setType("c8y_TemperatureMeasurement");
            measurementApi.create(measurementRepresentation);
        } catch (SDKException e) {
            logger.error("Error on creating Temperature Measurement", e);
        }
    }

    public void createBatteryMeasurement(ManagedObjectRepresentation mor, double batteryValue, DateTime dateTime) {
        try {
            MeasurementRepresentation measurementRepresentation = new MeasurementRepresentation();
            Battery battery = new Battery();
            battery.setLevelValue(BigDecimal.valueOf(batteryValue));
            measurementRepresentation.set(battery);
            measurementRepresentation.setSource(mor);
            measurementRepresentation.setDateTime(dateTime);
            measurementRepresentation.setType("c8y_BatteryMeasurement");
            measurementApi.create(measurementRepresentation);
        } catch (SDKException e) {
            logger.error("Error on creating Temperature Measurement", e);
        }
    }

    public void createSignalStrengthMeasurement(ManagedObjectRepresentation mor, long signalStrength, DateTime dataTime) {
        try {
            MeasurementRepresentation measurementRepresentation = new MeasurementRepresentation();
            SignalStrength sigStrength = new SignalStrength();
            sigStrength.setRssiValue(BigDecimal.valueOf(signalStrength));
            measurementRepresentation.set(sigStrength);
            measurementRepresentation.setSource(mor);
            measurementRepresentation.setDateTime(dataTime);
            measurementRepresentation.setType("c8y_SignalStrengthMeasurement");
            measurementApi.create(measurementRepresentation);
        } catch (SDKException e) {
            logger.error("Error on creating Signal Strength Measurement", e);
        }
    }

    public void createEvent(ManagedObjectRepresentation mor, String eventType, String eventText, boolean isOpen, DateTime dateTime) {
        try {
            EventRepresentation eventRepresentation = new EventRepresentation();
            eventRepresentation.setSource(mor);
            eventRepresentation.setDateTime(dateTime);
            eventRepresentation.setText(eventText);
            eventRepresentation.setType(eventType);
            eventRepresentation.setProperty("isOpen", isOpen);
            eventApi.create(eventRepresentation);
        } catch (SDKException e) {
            logger.error("Error on creating Event", e);
        }
    }

    @Scheduled(cron = "0 0 */1 * * *")
    public void createDailyMeasurements() {

        subscriptionsService.runForEachTenant(() -> {
            try {
                Iterator<ManagedObjectRepresentation> allDevicesIt = inventoryApi.getManagedObjects().get().allPages().iterator();
                while (allDevicesIt.hasNext()) {
                    ManagedObjectRepresentation mor = allDevicesIt.next();
                    if ("c8y_DTDevice".equals(mor.getType())) {
                        DateTime currentDataTime = DateTime.now();
                        DateTime toDate = currentDataTime.dayOfMonth().roundFloorCopy();
                        DateTime fromDate = toDate.minusDays(1);
                        EventFilter eventFilter = new EventFilter();
                        eventFilter.bySource(mor.getId());
                        eventFilter.byType("c8y_OpenCloseEvent");
                        eventFilter.byDate(new Date(fromDate.getMillis()), new Date(toDate.getMillis()));
                        EventCollection eventCollection = eventApi.getEventsByFilter(eventFilter);
                        int eventCounter = 0;
                        Iterator<EventRepresentation> eventIt = eventCollection.get().allPages().iterator();
                        while (eventIt.hasNext()) {
                            eventCounter++;
                        }
                        MeasurementRepresentation measurementRepresentation = new MeasurementRepresentation();
                        MeasurementValue mv = new MeasurementValue();
                        Map<String, MeasurementValue> measurementValueMap = new HashMap<>();
                        mv.setValue(BigDecimal.valueOf(eventCounter));
                        mv.setUnit("# per Day");
                        measurementValueMap.put("openCloseCountsPerDay", mv);
                        measurementRepresentation.set(measurementValueMap, "c8y_DailyCounter");
                        measurementRepresentation.setType("c8y_OpenCloseMeasurement");
                        measurementRepresentation.setSource(mor);
                        measurementRepresentation.setDateTime(toDate);
                        measurementApi.create(measurementRepresentation);
                    }
                }
            } catch (Exception e) {
                logger.error("Error on creating Counter Measurement");
            }
        });

    }
}
