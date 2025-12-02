/*
    This sketch shows the Ethernet event usage

*/

// Important to be defined BEFORE including ETH.h for ETH.begin() to work.
// Example RMII LAN8720 (Olimex, etc.)
#ifndef ETH_PHY_MDC
#define ETH_PHY_TYPE ETH_PHY_LAN8720
#if CONFIG_IDF_TARGET_ESP32
#define ETH_PHY_ADDR  0
#define ETH_PHY_MDC   23
#define ETH_PHY_MDIO  18
#define ETH_PHY_POWER -1
#define ETH_CLK_MODE  ETH_CLOCK_GPIO0_IN
#elif CONFIG_IDF_TARGET_ESP32P4
#define ETH_PHY_ADDR  0
#define ETH_PHY_MDC   31
#define ETH_PHY_MDIO  52
#define ETH_PHY_POWER 51
#define ETH_CLK_MODE  EMAC_CLK_EXT_IN
#endif
#endif

#include <ETH.h>
#include <WiFi.h>
#include <SPI.h>
#include <PubSubClient.h>
#include <functions.h>



WiFiClient espClient;
PubSubClient client(espClient);

const char* mqtt_client_id = "ESP32Publisher";
const char* topic = "spBv1.0/officeb/DDATA/ventilationchamber2/olimextemp";
const char* mqtt_server = "10.42.0.1";
const int mqtt_port = 1883;

/**
 * @brief 
 * 
 */


void connectMQTT() {
  while (!client.connected()) {
    Serial.println("Connecting to MQTT...");
    if (client.connect("ESP32Client")) {
      Serial.println("Connected to MQTT broker");
      // subscribe or publish here if needed  
    } else {
      Serial.print("Failed MQTT connection, state=");
      Serial.println(client.state());
      delay(2000);
    }
  }
}

bool eth_connected = false;

void WiFiEvent(arduino_event_id_t event) {
  switch (event) {
    case ARDUINO_EVENT_ETH_START:
      Serial.println("Ethernet started");
      ETH.setHostname("esp32-eth");
      break;
    case ARDUINO_EVENT_ETH_CONNECTED:
      Serial.println("Ethernet connected");
      break;
    case ARDUINO_EVENT_ETH_GOT_IP:
      Serial.print("IP Address: ");
      Serial.println(ETH.localIP());
      eth_connected = true;
      break;
    case ARDUINO_EVENT_ETH_DISCONNECTED:
      Serial.println("Ethernet disconnected");
      eth_connected = false;
      break;
    case ARDUINO_EVENT_ETH_STOP:
      Serial.println("Ethernet stopped");
      eth_connected = false;
      break;
    default:
      break;
  }
}

void networkBegin(){
    Network.onEvent(WiFiEvent);
    ETH.begin();
}

void connectMqtt(){
    if (!client.connected()) {
    connectMQTT();
  }
  client.loop();
}


void jsonPublish(){
    if (client.publish(topic, jsonBuffer)) {
      Serial.println("JSON published");
    } else {
      Serial.println("Publish failed"); 
    }   
}