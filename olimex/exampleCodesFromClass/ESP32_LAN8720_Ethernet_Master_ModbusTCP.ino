/*
Her er et simpelt eksempel på en lavniveau Modbus TCP klient (master) på ESP32 med Arduino IDE,
 som bruger TCP-socket direkte via WiFiClient til at sende en Modbus læseforespørgsel 
 (Function Code 0x03) - altså læs holding registers - til en Modbus TCP slave.
Forklaring:
Koden laver lav-niveau Modbus TCP: selv konstruerer og sender TCP-pakker til slave
Funktionen modbusReadHoldingRegisters bygger en simpel læse-forespørgsel (Function Code 3)
Svar læses råt og udskrives i hex til Serial Monitor
Ethernet med LAN8720 opsættes via ETH.begin og eventHandler
TCP-forbindelse til slave genbruges hvis mulig
*/

#include <ETH.h>
#include <WiFi.h>

// Ethernet PHY config (tilpas efter dit board)
#define ETH_PHY_ADDR 1
#define ETH_PHY_TYPE ETH_PHY_LAN8720
#define ETH_PHY_POWER 17
#define ETH_PHY_MDC 23
#define ETH_PHY_MDIO 18
#define ETH_CLK_MODE ETH_CLOCK_GPIO0_IN

// IP og port til Modbus TCP Slave
IPAddress modbusSlaveIP(192, 168, 137, 1);
const uint16_t modbusSlavePort = 502;

// Flag for Ethernet status
bool eth_connected = false;

// WiFiClient bruger TCP socket til Modbus
WiFiClient modbusClient;

// Simpel funktion til at sende Modbus TCP forespørgsel (Read Holding Registers)
// Ref: https://modbus.org/docs/Modbus_Application_Protocol_V1_1b.pdf
bool modbusReadHoldingRegisters(WiFiClient &client, uint16_t startReg, uint16_t numRegs) {
  if (!client.connected()) {
    if (!client.connect(modbusSlaveIP, modbusSlavePort)) {
      Serial.println("Modbus slave connect failed");
      return false;
    }
  }

  uint8_t packet[12];
  // MB TCP header
  packet[0] = 0x00; // Transaktions-ID high byte
  packet[1] = 0x01; // Transaktions-ID low byte
  packet[2] = 0x00; // Protocol ID high byte
  packet[3] = 0x00; // Protocol ID low byte
  packet[4] = 0x00; // Længde high byte (6 bytes efter header)
  packet[5] = 0x06; // Længde low byte
  packet[6] = 0x01; // Unit ID (slave address)
  packet[7] = 0x03; // Funktion 3 = Read Holding Registers

  packet[8] = highByte(startReg);
  packet[9] = lowByte(startReg);
  packet[10] = highByte(numRegs);
  packet[11] = lowByte(numRegs);

  client.write(packet, 12);
  client.flush();

  return true;
}

// Læs og vis Modbus svar (simples check uden parse!)
void modbusReadResponse(WiFiClient &client, uint16_t numRegs) {
  long start = millis();
  while (client.available() < (5 + 2 * numRegs)) {
    if (millis() - start > 2000) {
      Serial.println("Timeout waiting for Modbus response");
      client.stop();
      return;
    }
    delay(10);
  }

  uint8_t buffer[256];
  int len = client.read(buffer, client.available());
  
  Serial.print("Modbus response (hex): ");
  for (int i = 0; i < len; i++) {
    Serial.print(buffer[i], HEX);
    Serial.print(" ");
  }
  Serial.println();
}

void onEthernetEvent(arduino_event_id_t event, arduino_event_info_t info) {
  switch(event) {
    case ARDUINO_EVENT_ETH_START:
      Serial.println("ETH startet");
      ETH.setHostname("esp32-modbus-tcp-client");
      break;
    case ARDUINO_EVENT_ETH_CONNECTED:
      Serial.println("ETH forbundet");
      break;
    case ARDUINO_EVENT_ETH_GOT_IP:
      Serial.print("ETH IP-adresse: ");
      Serial.println(ETH.localIP());
      eth_connected = true;
      break;
    case ARDUINO_EVENT_ETH_DISCONNECTED:
      Serial.println("ETH afbrudt");
      eth_connected = false;
      break;
    case ARDUINO_EVENT_ETH_STOP:
      Serial.println("ETH stoppet");
      eth_connected = false;
      break;
    default:
      break;
  }
}

void setup() {
  Serial.begin(115200);
  delay(1000);
  Network.onEvent(onEthernetEvent);

  ETH.begin(ETH_PHY_TYPE, ETH_PHY_ADDR, ETH_PHY_MDC, ETH_PHY_MDIO, ETH_PHY_POWER, ETH_CLK_MODE);
}

void loop() {
  if (!eth_connected) {
    Serial.println("Venter på Ethernet forbindelse...");
    delay(1000);
    return;
  }

  Serial.println("Sender Modbus forespørgsel: Læs 2 holding registre fra adresse 0");
  if (modbusReadHoldingRegisters(modbusClient, 0, 2)) {
    modbusReadResponse(modbusClient, 2);
  } else {
    Serial.println("Kunne ikke sende Modbus forespørgsel");
  }

  delay(5000);
}
