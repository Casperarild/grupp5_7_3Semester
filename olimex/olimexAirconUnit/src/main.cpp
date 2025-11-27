/**
 * @file main.cpp
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2025-11-18
 * 
 * @copyright Copyright (c) 2025
 * 
 */
#include <Arduino.h>
#include <mqtt.h>
#include <functions.h>

/**
 * @brief testing setup
 * 
 */
void setup() { 
  Serial.begin(9600);
  modbusStartup();
}


/**
 * @brief testing loop
 * 
 */
void loop() {
  // if (!client.connected()) {
  //   reconnect();
  // }
  // client.loop();

  // if (airUnitAutoMode.regAddress == 0){
  //   airUnitAutoMode.regAddress = 1;
  //   writeHoldingRegister(airUnitAutoMode.regAddress, airUnitAutoMode.holdingRegs);
  // }
  // else{
    readAirconData();
    JsonDocument doc;

    // Add your data
    doc["airTemp"] = extractAirTemp.inputRegs;
    doc["humidtyOutdoor"] = humidityOutdoor.inputRegs;
    doc["humidtyRoom"] = humidtyRoom.inputRegs;
    doc["co2Sensor"] = co2Sensor.inputRegs;
    doc["supplyAirPressure"] = supplyAirPress.inputRegs;
    doc["supplyAirFlow"] = supplyAirFlow.inputRegs;
    doc["exhaustAirPressure"] = exhaustAirPress.inputRegs;
    doc["exhaustAirFlow"] = exhaustAirFlow.inputRegs;
    // Determine required size to hold JSON + '\0'
    size_t len = measureJson(doc) + 1;
    char jsonBuffer[len]; // create buffer on stack
    serializeJson(doc, jsonBuffer, len); // serialize JSON to char array
    Serial.println(jsonBuffer); // prints JSON as char array/string

    //client.publish(topic, jsonBuffer); 
    delay(500);
}

