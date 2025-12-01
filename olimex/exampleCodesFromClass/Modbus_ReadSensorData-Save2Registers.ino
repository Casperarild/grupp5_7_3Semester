#include <ModbusMaster.h>
#include <SoftwareSerial.h>

// Modbus configuration
#define MODBUS_DIR_PIN 5         // Connect DE, RE pins of MAX485 (if using RS485 module)
#define MODBUS_RX_PIN 16         // Rx pin for SoftwareSerial
#define MODBUS_TX_PIN 17         // Tx pin for SoftwareSerial
#define MODBUS_SERIAL_BAUD 9600  // Baud rate for Modbus RTU communication (match PC simulator settings)

// Set the Modbus slave address of the PC Modbus simulator
const uint8_t SLAVE_ID = 1;  // Use the same slave ID as set in the Modbus simulator

// Register addresses for Modbus communication
uint16_t holdingRegisterAddress = 0x0000;    // Holding register for digital output (coil write)
uint16_t inputRegisterAddress = 0x0001;      // Input register for digital input (coil read)

// Initialize the ModbusMaster object
ModbusMaster node;

// SoftwareSerial for Modbus communication
SoftwareSerial modbusSerial(MODBUS_RX_PIN, MODBUS_TX_PIN);

// Pin 5 made high for Modbus transmission mode (if using RS485 module)
void modbusPreTransmission() {
  delay(500);
  digitalWrite(MODBUS_DIR_PIN, HIGH);
}

// Pin 5 made low for Modbus receive mode (if using RS485 module)
void modbusPostTransmission() {
  digitalWrite(MODBUS_DIR_PIN, LOW);
  delay(500);
}

void setup() {
  // Initialize the built-in hardware serial communication
  Serial.begin(9600);  // Default settings: 9600bps, 8 data bits, no parity, 1 stop bit (match PC simulator settings)

  // Initialize SoftwareSerial for Modbus communication
  modbusSerial.begin(MODBUS_SERIAL_BAUD);

  // Set up the Modbus direction pin (only needed for RS485 communication)
  pinMode(MODBUS_DIR_PIN, OUTPUT);
  digitalWrite(MODBUS_DIR_PIN, LOW);

  // Initialize ModbusMaster with the slave ID of the PC Modbus slave simulator
  node.begin(SLAVE_ID, modbusSerial);  // Set the slave ID to match the PC Modbus slave simulator

  // Set pre and post transmission callbacks for RS485 transceiver configuration
  node.preTransmission(modbusPreTransmission);
  node.postTransmission(modbusPostTransmission);
}

void loop() {
  uint8_t result;
  uint16_t inputData;
  static bool outputState = false;  // Output state to toggle

  // --- Write Digital Output to Modbus Slave ---
  // Toggle the output state
  outputState = !outputState;
  
  // Write the digital output state (0 or 1) to the specified holding register of the Modbus slave
  result = node.writeSingleRegister(holdingRegisterAddress, outputState ? 1 : 0);  // Writing 1 for ON, 0 for OFF

  if (result == node.ku8MBSuccess) {
    Serial.println("Digital Output Write Success!");
  } else {
    Serial.print("Digital Output Write Failed, Response Code: ");
    Serial.println(result, HEX);
  }

  delay(1000);  // Add delay between operations

  // --- Read Digital Input from Modbus Slave ---
  // Read the digital input from the specified input register of the Modbus slave
  result = node.readInputRegisters(inputRegisterAddress, 1);  // Reading 1 input register

  if (result == node.ku8MBSuccess) {
    // Retrieve the input value from the response buffer
    inputData = node.getResponseBuffer(0x00);
    Serial.print("Digital Input Value: ");
    Serial.println(inputData);
  } else {
    Serial.print("Digital Input Read Failed, Response Code: ");
    Serial.println(result, HEX);
  }

  // Add a delay before the next iteration
  delay(2000);
}
