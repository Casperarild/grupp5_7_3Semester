/*
Her er en opdateret version af Arduino-lavniveau Modbus TCP klient,
 som udover at sende en læse-forespørgsel (Function Code 0x03)
 også parser og udskriver registerværdier korrekt:
00 01 00 00 00 07 01 03 04 00 6F 00 DE
  Byte 4 og 5 (0x00 0x07) angiver længden = 7 bytes (efter header)
  Byte 7: 0x01 (Unit ID)
  Byte 8: 0x03 (Funktionskode)
  Byte 9: 0x04 (Bytecount = 4, altså 4 bytes data)
  Byte 10-13: data (00 6F 00 DE) = to registre (2 x 2 bytes)

Byte indeks	Indhold:
  0-1	Transaktions ID
  2-3	Protokol ID (0x0000)
  4-5	Længde (antal bytes efter header)
  6	Unit ID (slaveadresse)
  7	Funktion
  8	Byte count (data længde)
  9+	Data (registre)
*/

#include <ETH.h>
#include <WiFi.h>

// Ethernet PHY konfiguration - tilpas efter hardware
#define ETH_PHY_ADDR 1
#define ETH_PHY_TYPE ETH_PHY_LAN8720
#define ETH_PHY_POWER 17
#define ETH_PHY_MDC 23
#define ETH_PHY_MDIO 18
#define ETH_CLK_MODE ETH_CLOCK_GPIO0_IN

// IP-adresse og port på Modbus slave
IPAddress modbusSlaveIP(192, 168, 137, 1);
const uint16_t modbusSlavePort = 502;

// Ethernet statusflag
bool eth_connected = false;

// TCP klient til Modbus
WiFiClient modbusClient;

// Funktion til at sende Modbus TCP læse-forespørgsel
bool modbusReadHoldingRegisters(WiFiClient &client, uint16_t startReg, uint16_t numRegs) {
  if (!client.connected()) {
    if (!client.connect(modbusSlaveIP, modbusSlavePort)) {
      Serial.println("Modbus slave connect failed");
      return false;
    }
  }

  uint8_t packet[12];
  packet[0] = 0x00; // Transaction ID high
  packet[1] = 0x01; // Transaction ID low
  packet[2] = 0x00; // Protocol ID high
  packet[3] = 0x00; // Protocol ID low
  packet[4] = 0x00; // Length high
  packet[5] = 0x06; // Length low (6 bytes efter header)
  packet[6] = 0x01; // Unit ID (slave address)
  packet[7] = 0x03; // Funktion 3: Læs Holding Registers
  packet[8] = highByte(startReg);
  packet[9] = lowByte(startReg);
  packet[10] = highByte(numRegs);
  packet[11] = lowByte(numRegs);

  client.write(packet, 12);
  client.flush();

  return true;
}

// Funktion til at modtage og parse Modbus svar
void modbusReadResponse(WiFiClient &client, uint16_t numRegs) {
  long start = millis();
  int expectedBytes = 9 + 2 * numRegs; // Minimum svarlængde

  while (client.available() < expectedBytes) {
    if (millis() - start > 2000) {
      Serial.println("Timeout waiting for Modbus response");
      client.stop();
      return;
    }
    delay(10);
  }

  uint8_t buffer[256];
  int len = client.read(buffer, client.available());

  Serial.print("Modbus rå svar (hex): ");
  for (int i = 0; i < len; i++) {
    if (buffer[i] < 0x10) Serial.print("0");
    Serial.print(buffer[i], HEX);
    Serial.print(" ");
  }
  Serial.println();

  if (len < expectedBytes) {
    Serial.println("Ugyldigt Modbus svar (for kort)");
    return;
  }

  int byteCount = buffer[8];
  Serial.print("Modtaget byteCount: ");
  Serial.println(byteCount);

  if (byteCount != numRegs * 2) {
    Serial.println("Forkert bytecount i Modbus svar");
    return;
  }

  Serial.print("Modbus registre værdier: ");
  for (int i = 0; i < numRegs; i++) {
    uint16_t regValue = (buffer[9 + 2 * i] << 8) | buffer[10 + 2 * i];
    Serial.print(regValue);
    if (i < numRegs - 1) Serial.print(", ");
  }
  Serial.println();
}

// Ethernet event handler
void onEthernetEvent(arduino_event_id_t event, arduino_event_info_t info) {
  switch (event) {
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

  Serial.println("Sender Modbus forespørgsel: læs 2 holding registre fra adresse 0");
  if (modbusReadHoldingRegisters(modbusClient, 0, 2)) {
    modbusReadResponse(modbusClient, 2);
  } else {
    Serial.println("Kunne ikke sende Modbus forespørgsel");
  }

  delay(5000);
}
