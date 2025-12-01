/*
 This sketch shows how to use lan8720 with esp32 core 3 with minimal/standard configuration
 In this example we are going to use https with insecure setting.
*/

#include <ETH.h>
#include <NetworkClientSecure.h>

// I²C-address of Ethernet PHY (0 or 1 for LAN8720, 31 for TLK110)
#define ETH_PHY_ADDR 1  // DEFAULT VALUE IS 0 YOU CAN OMIT IT
// Type of the Ethernet PHY (LAN8720 or TLK110)
#define ETH_PHY_TYPE ETH_PHY_LAN8720  // DEFAULT VALUE YOU CAN OMIT IT
// Pin# of the enable signal for the external crystal oscillator (-1 to disable for internal APLL source)
#define ETH_PHY_POWER 17  // DEFAULT VALUE YOU CAN OMIT IT
// Pin# of the I²C clock signal for the Ethernet PHY
#define ETH_PHY_MDC 23  // DEFAULT VALUE YOU CAN OMIT IT
// Pin# of the I²C IO signal for the Ethernet PHY
#define ETH_PHY_MDIO 18  // DEFAULT VALUE YOU CAN OMIT IT
// External clock from crystal oscillator
#define ETH_CLK_MODE ETH_CLOCK_GPIO0_IN  // DEFAULT VALUE YOU CAN OMIT IT

// Fixed value for RMI
//#define ETH_RMII_TX_EN  21
//#define ETH_RMII_TX0    19
//#define ETH_RMII_TX1    22
//#define ETH_RMII_RX0    25
//#define ETH_RMII_RX1_EN 26
//#define ETH_RMII_CRS_DV 27


static bool eth_connected = false;

// Ethernet event handler
void onEvent(arduino_event_id_t event, arduino_event_info_t info) {
  switch (event) {
    case ARDUINO_EVENT_ETH_START:
      Serial.println("ETH Started");
      // Set Ethernet hostname here
      ETH.setHostname("esp32-eth0");
      break;
    case ARDUINO_EVENT_ETH_CONNECTED:
      Serial.println("ETH Connected");
      break;
    case ARDUINO_EVENT_ETH_GOT_IP:
      Serial.printf("ETH Got IP: '%s'\n", esp_netif_get_desc(info.got_ip.esp_netif));
      Serial.println(ETH);
      eth_connected = true;
      break;
    case ARDUINO_EVENT_ETH_LOST_IP:
      Serial.println("ETH Lost IP");
      eth_connected = false;
      break;
    case ARDUINO_EVENT_ETH_DISCONNECTED:
      Serial.println("ETH Disconnected");
      eth_connected = false;
      break;
    case ARDUINO_EVENT_ETH_STOP:
      Serial.println("ETH Stopped");
      eth_connected = false;
      break;
    default:
      break;
  }
}

// Test client connection
void testClient(const char* host, uint16_t port) {
  Serial.print("\nconnecting to ");
  Serial.println(host);

  NetworkClientSecure client;
  client.setInsecure();

  if (!client.connect(host, port)) {
    Serial.println("connection failed");
    return;
  }
  client.println("GET /get HTTP/1.1");
  client.println("Host: example.com");
  client.println("Connection: close");
  client.println();

  while (client.connected() && !client.available())
    ;
  while (client.available()) {
    Serial.write(client.read());
  }

  Serial.println("closing connection\n");
  client.stop();
}

void setup() {
  Serial.begin(115200);
  Network.onEvent(onEvent);

  //  ETH.begin();
  ETH.begin(ETH_PHY_TYPE, ETH_PHY_ADDR, ETH_PHY_MDC, ETH_PHY_MDIO, ETH_PHY_POWER, ETH_CLK_MODE);
}

void loop() {
  if (eth_connected) {
    testClient("example.com", 443);
  }
  delay(10000);
}