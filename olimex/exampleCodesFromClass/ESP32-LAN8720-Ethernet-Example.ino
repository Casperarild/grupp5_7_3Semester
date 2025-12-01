/*
   ESP32 LAN8720 Ethernet Example
   Compatible with ESP32 Arduino Core v2.x/v3.x
   LAN8720 PHY, inverted clock on GPIO17, MDC=23, MDIO=18
   All parameters and event types are correct for Arduino Core v2.x/v3.x.

You may adjust ETH_PHY_ADDR if your LAN8720 module uses address 0 instead of 1.​
The code will print debug information and the HTTP response to Serial Monitor when a connection is made.
*/

#include <ETH.h>
#include <WiFi.h>

// LAN8720 parameters (update if your board is different)
#define ETH_PHY_ADDR      1                // Usually 0 or 1 (check jumper/solder bridge)
#define ETH_PHY_TYPE      ETH_PHY_LAN8720  // Use ETH_PHY_LAN8720 for LAN8720
#define ETH_PHY_POWER     -1               // Use -1 if EN pin is not connected
#define ETH_PHY_MDC       23               // MDC pin (default for LAN8720)
#define ETH_PHY_MDIO      18               // MDIO pin (default for LAN8720)
#define ETH_CLK_MODE      ETH_CLOCK_GPIO17_OUT // Inverted clock on GPIO17 (recommended for most LAN8720 modules)

static bool eth_connected = false;

// Event handler for Ethernet events
void WiFiEvent(WiFiEvent_t event) {
  switch (event) {
    case ARDUINO_EVENT_ETH_START:
      Serial.println("ETH Started");
      ETH.setHostname("esp32-ethernet");
      break;
    case ARDUINO_EVENT_ETH_CONNECTED:
      Serial.println("ETH Connected");
      break;
    case ARDUINO_EVENT_ETH_GOT_IP:
      Serial.print("ETH MAC: ");
      Serial.print(ETH.macAddress());
      Serial.print(", IPv4: ");
      Serial.print(ETH.localIP());
      if (ETH.fullDuplex()) {
        Serial.print(", FULL_DUPLEX");
      }
      Serial.print(", ");
      Serial.print(ETH.linkSpeed());
      Serial.println("Mbps");
      eth_connected = true;
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

// Simple test: connect via HTTP client and print results
void testClient(const char *host, uint16_t port) {
  Serial.print("\nconnecting to ");
  Serial.println(host);

  WiFiClient client;
  if (!client.connect(host, port)) {
    Serial.println("connection failed");
    return;
  }
  client.printf("GET / HTTP/1.1\r\nHost: %s\r\n\r\n", host);
  while (client.connected() && !client.available())
    delay(1); // prevents WDT reset
  while (client.available())
    Serial.write(client.read());

  Serial.println("closing connection\n");
  client.stop();
}

void setup() {
  Serial.begin(115200);
  WiFi.onEvent(WiFiEvent); // Attach event handler
  ETH.begin(
    ETH_PHY_TYPE,      // Use the enum, not int!
    ETH_PHY_ADDR,
    ETH_PHY_MDC,
    ETH_PHY_MDIO,
    ETH_PHY_POWER,
    ETH_CLK_MODE
  );
}

void loop() {
  if (eth_connected) {
    testClient("google.com", 80);
  }
  delay(10000); // Retry every 10 seconds
}
/*
| ESP32 Pin | LAN8720 Signal | Notes                 |
| --------- | -------------- | --------------------- |
| GPIO17    | CLK            | 50MHz, inverted clock |
| GPIO23    | MDC            | Clock input           |
| GPIO18    | MDIO           | Bi-directional data   |
| GPIO21    | TX_EN          | EMAC_TX_EN            |
| GPIO19    | TX0            | EMAC_TXD0             |
| GPIO22    | TX1            | EMAC_TXD1             |
| GPIO25    | RX0            | EMAC_RXD0             |
| GPIO26    | RX1            | EMAC_RXD1             |
| GPIO27    | CRS_DV         | EMAC_RX_DRV           |
*/