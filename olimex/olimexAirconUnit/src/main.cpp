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

String Modtaget_data = "off";
int mode = 0;


/**
 * @brief testing setup
 * 
 */
void setup() { 
  Serial.begin(9600);
  networkBegin();
  mqttBegin();
  modbusStartup();
}


/**
 * @brief testing loop
 * 
 */
void loop() {
  uint8_t result;
  if (!client.connected()) {
    reconnect();
  }

  while (Serial.available() > 0) {
    delay(200);  // Give some time for the entire message to arrive
    Modtaget_data = Serial.readStringUntil('\n');
    Modtaget_data.trim();  // Remove any whitespace
    //Serial.flush();        // Clear the serial buffer
    mode = Modtaget_data.toInt();
    Serial.println("Received data: " + Modtaget_data);
    
    switch (mode){
      case 0:
        Serial.println("Anlæg slukker");
          result = modbus.writeSingleRegister(367, 0);
          delay(100);
          if (result == modbus.ku8MBSuccess) {
            Serial.println("Successfully wrote 0 to register 368");
        } else {
          Serial.print("Modbus error: ");
          Serial.println(result);
        }
        break;
      case 1:
        Serial.println("Anlæg tænder på lav kraft");
        result = modbus.writeSingleRegister(367, 1);
        delay(100);
        if (result == modbus.ku8MBSuccess) {
          Serial.println("Successfully wrote 1 to register 368");
          
        } else {
          Serial.print("Modbus error: ");
          Serial.println(result);
        }
        break;
      case 2:
        Serial.println("Anlæg tænder på normal kraft");
        result = modbus.writeSingleRegister(367, 2);
        delay(100);
        if (result == modbus.ku8MBSuccess) {
          Serial.println("Successfully wrote 2 to register 368");
          
        } else {
          Serial.print("Modbus error: ");
          Serial.println(result);
        }
        break;
      case 3:
        Serial.println("Anlæg tænder i auto mode");
        result = modbus.writeSingleRegister(367, 3);
        delay(100);
        if (result == modbus.ku8MBSuccess) {
          Serial.println("Successfully wrote 3 to register 368");
          
        } else {
          Serial.print("Modbus error: ");
          Serial.println(result);
        }
        break;
      }
  }
  client.loop();
  if (mode !=0){
    dataToJson();
    delay(200);
  }
  delay(200);
}

  // if (airUnitAutoMode.regAddress == 0){
  //   airUnitAutoMode.regAddress = 1;
  //   writeHoldingRegister(airUnitAutoMode.regAddress, airUnitAutoMode.holdingRegs);
  // }
  // else{
