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
  String id;
  String unit;
  float value;
};  

struct holdReg
{
  int regAddress;
  uint16_t holdingRegs;
  const char* name;
};

inputReg outdoorTemp = {0, inputRegs[2], "Temperature outside: ", "C"};
inputReg extractAirTemp{8, inputRegs[2], "Temperature inside: ", "C"};
inputReg humidityOutdoor{154, inputRegs[2], "Humidity outside: ", "% RH"};
inputReg humidtyRoom{22, inputRegs[2], "Humidty inside: ", "% RH"};
inputReg supplyAirPress{12, inputRegs[2], "Airpressure supplied to unit: ", "Pa"};
inputReg exhaustAirPress{13, inputRegs[2], "Exhaust airpressure: ", "Pa"};
inputReg supplyAirFlow{14, inputRegs[2], "Air flow on supply: ", "m³/h"};
inputReg exhaustAirFlow{15, inputRegs[2], "Air flow fom exhaust: ", "m³/h"};
holdReg airUnitAutoMode{367, holdingRegs[2], "Aircon speed control: "};
//inputReg co2Sensor{16, inputRegs[20], "CO2 sensor"}; //Register not available on aircon unit


/*
Reference values for modbus simulation:
Input register values (3x)
1 = 50
9 = 230
13 = 260
14 = 240
15 = 490
16 = 470
23 = 500    
155 = 560
*/

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

// Function to write holding registers (e.g., setpoints)
void writeHoldingRegister(int reg, uint16_t val ) {
    // Prepare setpoint values
    // Load transmit buffer with setpoint values
    modbus.setTransmitBuffer(0, val);
    // Attempt to write 2 holding registers starting at address 3
    if (modbus.writeSingleRegister(reg, 1) == modbus.ku8MBSuccess) {
        Serial.println("Holding Registers Updated Successfully");
    } else {
        Serial.println("ERROR: Failed to write holding registers");
    }
}

void airconDataReadPrint(float val, String id, String unit){
    Serial.print(id);
    Serial.print(val);
    Serial.print(unit);
    Serial.println();
    delay(500);
}

/**
 * @brief 
 * 
 * 
 */
void readAirconData(){      
    //float signedRaw;

    /**
     * @brief Negative temp function
     * 
     */
    uint16_t outdoorTemps = readInputRegister(outdoorTemp.regAddress);
    if (outdoorTemps > 32767){
        int16_t signedRaw = outdoorTemps - 65536;
        outdoorTemp.value = signedRaw / 10.0;
        signedRaw = 0;
    } else {
        outdoorTemp.value = outdoorTemp.inputRegs / 10.0;
    }
    airconDataReadPrint(outdoorTemp.value, outdoorTemp.id, outdoorTemp.unit);
    delay(500);

    uint16_t airTemps = readInputRegister(extractAirTemp.regAddress);
    if (airTemps > 32767){
        int16_t signedRaw = airTemps - 65536;
        extractAirTemp.value = signedRaw / 10.0;
        signedRaw = 0;
    } else {
        extractAirTemp.inputRegs = extractAirTemp.inputRegs / 10.0;
    }
    airconDataReadPrint(extractAirTemp.value, extractAirTemp.id, extractAirTemp.unit);
    delay(500);

    humidityOutdoor.value = readInputRegister(humidityOutdoor.regAddress) / 10.0;
    airconDataReadPrint(humidityOutdoor.value, humidityOutdoor.id, humidityOutdoor.unit);
    delay(500);

    humidtyRoom.value = readInputRegister(humidtyRoom.regAddress) / 10.0;
    airconDataReadPrint(humidtyRoom.value, humidtyRoom.id, humidtyRoom.unit);
    delay(500);

    supplyAirFlow.value = readInputRegister(supplyAirFlow.regAddress) / 10.0;
    airconDataReadPrint(supplyAirFlow.value, supplyAirFlow.id, supplyAirFlow.unit);
    delay(500);
    
    supplyAirPress.value = readInputRegister(supplyAirPress.regAddress) / 10.0;
    airconDataReadPrint(supplyAirPress.value, supplyAirPress.id, supplyAirPress.unit);
    delay(500);

    exhaustAirFlow.value = readInputRegister(exhaustAirFlow.regAddress) / 10.0;
    airconDataReadPrint(exhaustAirFlow.value, exhaustAirFlow.id, exhaustAirFlow.unit);
    delay(500);
    
    exhaustAirPress.value = readInputRegister(exhaustAirPress.regAddress) / 10.0;
    airconDataReadPrint(exhaustAirPress.value, exhaustAirPress.id, exhaustAirPress.unit);
    delay(500);

    //co2Sensor.inputRegs = readInputRegister(co2Sensor.regAddress);
    //Serial.println("Air temp: " + co2Sensor.inputRegs);
    //delay(500);
}


void clearValBuffer(){
    outdoorTemp.value = 0;
    extractAirTemp.value = 0;
    humidityOutdoor.value = 0;
    humidtyRoom.value = 0;
    supplyAirFlow.value = 0;
    supplyAirPress.value = 0;
    exhaustAirFlow.value = 0;
    exhaustAirPress.value = 0;
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

    doc["outdoorTemp"] = outdoorTemp.value;
    doc["airTemp"] = extractAirTemp.value;
    doc["humidtyOutdoor"] = humidityOutdoor.value;
    doc["humidtyRoom"] = humidtyRoom.value;
    //doc["co2Sensor"] = co2Sensor.inputRegs;
    doc["supplyAirPressure"] = supplyAirPress.value;
    doc["supplyAirFlow"] = supplyAirFlow.value;
    doc["exhaustAirPressure"] = exhaustAirPress.value;
    doc["exhaustAirFlow"] = exhaustAirFlow.value;

    // Determine required size to hold JSON + '\0'
    serializeJson(doc, jsonBuffer, sizeof(jsonBuffer));  // Use fixed size
    jsonBuffer[sizeof(jsonBuffer)-1] = '\0';  // Ensure null termination
    Serial.println(jsonBuffer); // prints JSON as char array/string

    delay(500);
}

void modbusAirconModeCheck(){ 
    uint16_t targetVal = 2;
    uint8_t readResult = modbus.readHoldingRegisters(367, 1);
    if (readResult == modbus.ku8MBSuccess) {
        uint16_t holdingValue = modbus.getResponseBuffer(0);
        if (holdingValue == 0) {
            Serial.println("Anlæg tænder på normal kraft");
            uint8_t matchVal = modbus.writeSingleRegister(367, 2);
        if (matchVal == modbus.ku8MBSuccess) {
            Serial.println("Successfully wrote 2 to register 368");
            delay(500);
        } else {
            Serial.print("Modbus error: ");
            delay(400);
            } 
        }
    }
}