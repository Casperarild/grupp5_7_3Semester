#include <PubSubClient.h>
#include <WiFi.h>

WiFiClient espClient;
PubSubClient client(espClient);

const char* topic = "spBv1.0/officeb/DDATA/ventilationchamber2/olimextemp";
const char* ssid = "tryk_hotspot";
const char* password = "swewxpe7";

const char* mqtt_server = "192.168.73.84"; // Replace with your MQTT broker IP
const int mqtt_port = 1883;

/**
 * @brief 
 * 
 */
void reconnect() {
    while (!client.connected()) {
        Serial.print("Connecting to MQTT...");
        Serial.print(" Connecting To MQTT Broker");
    if (client.connect("ESP32Publisher")) {
        Serial.println("MQTT connected");
    } else {
        Serial.print("MQTT Failed To Connect");
      delay(8000);
    }
  }
}