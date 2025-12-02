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
  connectMQTT();

  modbusAirconModeCheck();

  dataToJson();
  delay(400);

  jsonPublish();
  delay(4000);

  clearValBuffer();
  delay(200);
}