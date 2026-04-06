#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h>

// =========================
// DS18B20 Temperature Sensor
// =========================
// red    -> 3.3V
// black  -> GND
// yellow -> GPIO4

#define ONE_WIRE_BUS 4

OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature tempSensors(&oneWire);

// =========================
// ADXL345 Vibration Sensor
// =========================
// VCC -> 3.3V
// GND -> GND
// SDA -> GPIO21
// SCL -> GPIO22
// CS  -> 3.3V
// SDO -> GND

#define ADXL345_ADDR 0x53

int readingIndex = 1;

// -------------------------
// ADXL345 helper functions
// -------------------------
void writeRegister(uint8_t reg, uint8_t value) {
  Wire.beginTransmission(ADXL345_ADDR);
  Wire.write(reg);
  Wire.write(value);
  Wire.endTransmission();
}

void readRegisters(uint8_t startReg, uint8_t count, uint8_t *data) {
  Wire.beginTransmission(ADXL345_ADDR);
  Wire.write(startReg);
  Wire.endTransmission(false);

  Wire.requestFrom(ADXL345_ADDR, count);

  uint8_t i = 0;
  while (Wire.available() && i < count) {
    data[i++] = Wire.read();
  }
}

void setup() {
  Serial.begin(115200);
  delay(1000);

  // DS18B20 setup
  pinMode(ONE_WIRE_BUS, INPUT_PULLUP);
  tempSensors.begin();

  Serial.print("DS18B20 device count = ");
  Serial.println(tempSensors.getDeviceCount());

  // ADXL345 setup
  Wire.begin(21, 22);
  delay(500);

  // Put ADXL345 into measurement mode
  writeRegister(0x2D, 0x08);

  // Full resolution, +/-2g
  writeRegister(0x31, 0x08);

  // Optional: quick ADXL345 presence check
  Wire.beginTransmission(ADXL345_ADDR);
  if (Wire.endTransmission() == 0) {
    Serial.println("ADXL345 detected and initialized.");
  } else {
    Serial.println("ADXL345 NOT detected.");
  }

  Serial.println("Both sensors setup complete.");
  Serial.println("------------------------------------");
}

void loop() {
  // =========================
  // Read temperature
  // =========================
  tempSensors.requestTemperatures();
  float tempC = tempSensors.getTempCByIndex(0);

  // =========================
  // Read ADXL345 vibration
  // =========================
  uint8_t rawData[6];
  readRegisters(0x32, 6, rawData);

  int16_t x = (int16_t)((rawData[1] << 8) | rawData[0]);
  int16_t y = (int16_t)((rawData[3] << 8) | rawData[2]);
  int16_t z = (int16_t)((rawData[5] << 8) | rawData[4]);

  float x_g = x * 0.0039;
  float y_g = y * 0.0039;
  float z_g = z * 0.0039;

  // =========================
  // Print all together
  // =========================
  Serial.print("Reading ");
  Serial.println(readingIndex);

  Serial.print("Temperature (C): ");
  Serial.println(tempC);

  Serial.print("Raw X: ");
  Serial.print(x);
  Serial.print(" | Y: ");
  Serial.print(y);
  Serial.print(" | Z: ");
  Serial.println(z);

  Serial.print("g X: ");
  Serial.print(x_g, 3);
  Serial.print(" | Y: ");
  Serial.print(y_g, 3);
  Serial.print(" | Z: ");
  Serial.println(z_g, 3);

  Serial.println("------------------------------------");

  readingIndex++;
  delay(1000);
}