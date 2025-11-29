// /**
//  * @file main.cpp
//  * @author your name (you@domain.com)
//  * @brief 
//  * @version 0.1
//  * @date 2025-11-18
//  * 
//  * @copyright Copyright (c) 2025
//  * 
//  */
#include <Arduino.h>
#include <WiFi.h>
#include <ETH.h>
#include <PubSubClient.h>
#include <mqtt.h>

//String Modtaget_data = "off";
int mode = 2;


// /**
//  * @brief testing setup
//  * 
//  */
void setup() { 
  Serial.begin(9600);
  networkBegin();
  ETH.begin();  // initialize Ethernet
  client.setServer(mqtt_server, mqtt_port);
  connectMQTT();
  modbusStartup();
}


// /**
//  * @brief testing loop
//  * 
//  */


void loop() {
  uint8_t result;
  if (!client.connected()) {
    connectMQTT();
  }
  client.loop();
  if (mode = 2){
    mode == 0;
    Serial.println("Anlæg tænder på normal kraft");
    result = modbus.writeSingleRegister(367, 2);
    delay(100);
    if (result == modbus.ku8MBSuccess) {
      Serial.println("Successfully wrote 2 to register 368");
    } else {
      Serial.print("Modbus error: ");
      Serial.println(result);
    }
  }
  dataToJson();
  delay(400);
  if (client.publish(topic, jsonBuffer)) {
    Serial.println("JSON published");
  } else {
    Serial.println("Publish failed"); 
  }   
  delay(4000);
  clearValBuffer();
  delay(200);
}
  

//   // if (airUnitAutoMode.regAddress == 0){
//   //   airUnitAutoMode.regAddress = 1;
//   //   writeHoldingRegister(airUnitAutoMode.regAddress, airUnitAutoMode.holdingRegs);
//   // }
//   // else{

