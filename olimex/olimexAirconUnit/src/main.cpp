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

String Modtaget_data = "on";

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

    uint8_t result;

  while (Serial.available() > 0) {
    delay(10);  // Give some time for the entire message to arrive
    Modtaget_data = Serial.readStringUntil('\n');
    Modtaget_data.trim();  // Remove any whitespace
    //Serial.flush();        // Clear the serial buffer

    Serial.println("Received data: " + Modtaget_data);
    if (Modtaget_data == "on") {
      Serial.println("Anlæg tænder");
      result = modbus.writeSingleRegister(367, 1);
      //test
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
      if (result == modbus.ku8MBSuccess) {
        Serial.println("Successfully wrote 3 to register 368");
        
      } else {
        Serial.print("Modbus error: ");
        Serial.println(result);
      }
    } else if (Modtaget_data == "off") {
      Serial.println("Anlæg slukker");
      result = modbus.writeSingleRegister(367, 0);
      if (result == modbus.ku8MBSuccess) {
        Serial.println("Successfully wrote 0 to register 368");
      } else {
        Serial.print("Modbus error: ");
        Serial.println(result);
      }
    }

  // if (!client.connected()) {
  //   reconnect();
  // }
  // client.loop();

  // if (airUnitAutoMode.regAddress == 0){
  //   airUnitAutoMode.regAddress = 1;
  //   writeHoldingRegister(airUnitAutoMode.regAddress, airUnitAutoMode.holdingRegs);
  // }
  // else{
  }
}

