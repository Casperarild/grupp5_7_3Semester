#include <ModbusMaster.h>
#include <ArduinoJson.h>

// ================ MODBUS COMMUNICATION CONFIGURATION ================
#define RX_PIN 36         // UART2 RX pin 36 for olimex 16 for esp
#define TX_PIN 4           // UART2 TX pin 4 for olimex 17 for esp
#define MAX485_DE 5         // RS485 Driver Enable pin
#define MAX485_RE_NEG 14    // RS485 Receiver Enable pin (active low)
#define BAUD_RATE 9600      // Communication speed
#define MODBUS_SLAVE_ID 1   // Slave device address

// ================ DATA BUFFERS ================
uint16_t holdingRegs[2];    // Buffer for holding registers (writable)
uint16_t inputRegs[2];      // Buffer for input registers (read-only)
bool discreteInputs[2];     // Buffer for discrete inputs (binary status)
char frontDoorStatus[10];   // Human-readable front door status
char backDoorStatus[10];    // Human-readable back door status

JsonDocument doc;


// Create Modbus master object
ModbusMaster modbus;

struct inputReg
{
  int regAddress;
  uint16_t inputRegs;
  const char* name;
};  

struct holdReg
{
  int regAddress;
  uint16_t holdingRegs;
  const char* name;
};

inputReg outdoorTemp = {0, inputRegs[2], "Temperature outside"};
inputReg extractAirTemp{8, inputRegs[2], "Temperature inside"};
inputReg humidityOutdoor{154, inputRegs[2], "Humidity outside"};
inputReg humidtyRoom{22, inputRegs[2], "Humidty inside"};
//inputReg co2Sensor{16, inputRegs[20], "CO2 sensor"}; //Register not available on aircon unit
inputReg supplyAirPress{12, inputRegs[2], "Airpressure supplied to unit"};
inputReg exhaustAirPress{13, inputRegs[2], "Exhaust airpressure"};
inputReg supplyAirFlow{14, inputRegs[2], "Air flow on supply"};
inputReg exhaustAirFlow{15, inputRegs[2], "Air flow fom exhaust "};
holdReg airUnitAutoMode{367, holdingRegs[2], "Aircon speed control"};

// Function to prepare for data transmission
void preTransmission() {
    digitalWrite(MAX485_RE_NEG, HIGH);  // Disable receiver
    digitalWrite(MAX485_DE, HIGH);      // Enable driver
}

// Function to clean up after data transmission
void postTransmission() {
    digitalWrite(MAX485_RE_NEG, LOW);   // Enable receiver
    digitalWrite(MAX485_DE, LOW);       // Disable driver
}


// Function to read input registers (e.g., motor RPM)
uint16_t readInputRegister(int reg) {
    uint16_t val;
    if (modbus.readInputRegisters(reg, 1) == modbus.ku8MBSuccess) {
        //Serial.println(modbus.getResponseBuffer(0));
        val = modbus.getResponseBuffer(0);
    } else {
        Serial.println("ERROR: Failed to read input registers");
        val = 8009; // Set error value
    }
    return val;
}


// Function to read discrete inputs (e.g., door statuses)
void readDiscreteInputs() {
    // Attempt to read 2 discrete inputs starting at address 1
    if (modbus.readDiscreteInputs(1, 2) == modbus.ku8MBSuccess) {
        // Store door states (note: response buffer indexing starts at 0)
        discreteInputs[0] = modbus.getResponseBuffer(0);
        discreteInputs[1] = modbus.getResponseBuffer(1);

        // Convert boolean states to human-readable strings
        snprintf(frontDoorStatus, sizeof(frontDoorStatus), "%s", discreteInputs[0] ? "Open" : "Closed");
        snprintf(backDoorStatus, sizeof(backDoorStatus), "%s", discreteInputs[1] ? "Closed" : "Open");

        // Display door statuses
        Serial.printf("Front Door: %s\n", frontDoorStatus);
        Serial.printf("Back Door: %s\n", backDoorStatus);
    } else {
        Serial.println("ERROR: Failed to read discrete inputs");
    }
}

// Function to write holding registers (e.g., setpoints)
uint16_t writeHoldingRegister(int reg, uint16_t val ) {
    // Prepare setpoint values
    // Load transmit buffer with setpoint values
    modbus.setTransmitBuffer(0, val);

    // Attempt to write 2 holding registers starting at address 3
    if (modbus.writeSingleRegister(reg, 1) == modbus.ku8MBSuccess) {
        Serial.println("Holding Registers Updated Successfully");
        return val;
    } else {
        Serial.println("ERROR: Failed to write holding registers");
        return 0;
    }
}

void readAirconData(){  
    outdoorTemp.inputRegs = readInputRegister(outdoorTemp.regAddress / 10);
    Serial.println(outdoorTemp.inputRegs);
    delay(500);
    extractAirTemp.inputRegs = readInputRegister(extractAirTemp.regAddress / 10.0);
    Serial.print("Air temp: ");
    Serial.print(extractAirTemp.inputRegs);
    Serial.println("C");
    delay(500);
    humidityOutdoor.inputRegs = readInputRegister(humidityOutdoor.regAddress);
    Serial.println(humidityOutdoor.inputRegs);
    delay(500);
    humidtyRoom.inputRegs = readInputRegister(humidtyRoom.regAddress);
    Serial.println(humidtyRoom.inputRegs);
    delay(500);
    //co2Sensor.inputRegs = readInputRegister(co2Sensor.regAddress);
    //Serial.println("Air temp: " + co2Sensor.inputRegs);
    //delay(500);
    supplyAirFlow.inputRegs = readInputRegister(supplyAirFlow.regAddress);
    Serial.println(supplyAirFlow.inputRegs);
    delay(500);
    supplyAirPress.inputRegs = readInputRegister(supplyAirPress.regAddress);
    Serial.println(supplyAirPress.inputRegs);
    delay(500);
    exhaustAirFlow.inputRegs = readInputRegister(exhaustAirFlow.regAddress);
    Serial.println(exhaustAirFlow.inputRegs);
    delay(500);
    exhaustAirPress.inputRegs = readInputRegister(exhaustAirPress.regAddress);
    Serial.println(exhaustAirPress.inputRegs);
    delay(500);
}

void clearValBuffer(){
    outdoorTemp.inputRegs = 0;
    extractAirTemp.inputRegs = 0;
    humidityOutdoor.inputRegs = 0;
    humidtyRoom.inputRegs = 0;
    supplyAirFlow.inputRegs = 0;
    supplyAirPress.inputRegs = 0;
    exhaustAirFlow.inputRegs = 0;
    exhaustAirPress.inputRegs = 0;
}


void modbusStartup(){
        // Initialize RS485 control pins
    pinMode(MAX485_RE_NEG, OUTPUT);
    pinMode(MAX485_DE, OUTPUT);
    digitalWrite(MAX485_RE_NEG, LOW);
    digitalWrite(MAX485_DE, LOW);
    Serial.println("ESP32 Modbus RTU Communication Initializing...");

    // Configure UART2 for Modbus communication
    Serial2.begin(BAUD_RATE, SERIAL_8N1, RX_PIN, TX_PIN);

    // Initialize Modbus master with slave ID and UART2
    modbus.begin(MODBUS_SLAVE_ID, Serial2);

    // Set pre- and post-transmission callbacks for RS485 control
    modbus.preTransmission(preTransmission);
    modbus.postTransmission(postTransmission);

    Serial.println("Modbus RTU Communication Initialized Successfully");

}

const char* packet;

char jsonBuffer[1024];

void dataToJson(){
    readAirconData();

    doc["outdoorTemp"] = outdoorTemp.inputRegs;
    doc["airTemp"] = extractAirTemp.inputRegs;
    doc["humidtyOutdoor"] = humidityOutdoor.inputRegs;
    doc["humidtyRoom"] = humidtyRoom.inputRegs;
    //doc["co2Sensor"] = co2Sensor.inputRegs;
    doc["supplyAirPressure"] = supplyAirPress.inputRegs;
    doc["supplyAirFlow"] = supplyAirFlow.inputRegs;
    doc["exhaustAirPressure"] = exhaustAirPress.inputRegs;
    doc["exhaustAirFlow"] = exhaustAirFlow.inputRegs;

    // Determine required size to hold JSON + '\0'
    serializeJson(doc, jsonBuffer, sizeof(jsonBuffer));  // Use fixed size
    jsonBuffer[sizeof(jsonBuffer)-1] = '\0';  // Ensure null termination
    Serial.println(jsonBuffer); // prints JSON as char array/string

    delay(500);
}


