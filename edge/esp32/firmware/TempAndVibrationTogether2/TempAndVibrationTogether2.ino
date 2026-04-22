#include <WiFi.h>
#include <ArduinoOTA.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h>
#include <PubSubClient.h>
#include <math.h>

// ======================================================
// SENSOR / WIRING NOTES
// ======================================================
// DS18B20
// red    -> 3.3V
// black  -> GND
// yellow -> GPIO4
//
// ADXL345
// VCC -> 3.3V
// GND -> GND
// SDA (green) -> GPIO21
// SCL (yellow) -> GPIO22
// CS  -> 3.3V
// SDO -> GND
//
// IMPORTANT:
// - ADXL345 is configured for FULL_RES + +/-2g
// - scale factor = 3.9 mg/LSB = 0.0039 g/LSB

// ======================================================
// WIFI CONFIG
// ======================================================
const char* ssid = "Klea";
const char* password = "rinesanart";

// ======================================================
// MQTT CONFIG
// ======================================================
const char* mqttServer = "192.168.178.142";   // Pi4 IP
const int mqttPort = 1883;
const char* mqttTopic = "factory/factory-001/machine/machine-001/telemetry";
const char* mqttClientId = "esp32-002";

// ======================================================
// IDs
// ======================================================
const char* factoryId = "factory-001";
const char* machineId = "machine-001";
const char* deviceId = "esp32-002";

// ======================================================
// TEST / SIMULATION CONFIG
// ======================================================
bool SIMULATE_SENSORS = true;   // true = no physical sensors needed
bool FORCE_ANOMALY = false;     // false for initial testing

// ======================================================
// ANOMALY SIMULATION CONFIG
// ======================================================
enum AnomalyMode {
  ANOM_NONE = 0,
  ANOM_TEMP_ONLY = 1,
  ANOM_VIB_ONLY = 2,
  ANOM_BOTH = 3
};

struct AnomalySegment {
  uint32_t startIndex;
  uint32_t count;
  AnomalyMode mode;
};

const AnomalySegment ANOMALY_SCHEDULE[] = {
  {30001, 18000, ANOM_TEMP_ONLY},
  {66001, 18000, ANOM_VIB_ONLY},
  {102001, 18000, ANOM_BOTH}
};

const size_t ANOMALY_SCHEDULE_COUNT =
  sizeof(ANOMALY_SCHEDULE) / sizeof(ANOMALY_SCHEDULE[0]);

// Forced anomaly values
const float FORCED_TEMP_C = 38.0f;
const float FORCED_X_G = 0.90f;
const float FORCED_Y_G = 0.85f;
const float FORCED_Z_G = 0.20f;

// Simulated baseline values
const float SIM_TEMP_BASE_C = 24.5f;
const float SIM_Z_BASE_G = 1.00f;

// ======================================================
// DS18B20
// ======================================================
#define ONE_WIRE_BUS 4
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature tempSensors(&oneWire);

// ======================================================
// ADXL345
// ======================================================
#define ADXL345_ADDR 0x53
#define ADXL345_DEVID_REG 0x00
#define ADXL345_POWER_CTL 0x2D
#define ADXL345_DATA_FORMAT 0x31
#define ADXL345_BW_RATE 0x2C
#define ADXL345_DATAX0 0x32

const float ADXL345_SCALE_G_PER_LSB = 0.0039f; // full-res +/-2g

// ======================================================
// WIFI / MQTT OBJECTS
// ======================================================
WiFiClient espClient;
PubSubClient mqttClient(espClient);

// ======================================================
// TIMING
// ======================================================
// 1000 ms for testing, easier to observe in Serial + MQTT
// change back to 10 for 100 Hz later
const unsigned long publishIntervalMs = 10;

// Read temperature asynchronously
const unsigned long tempConversionWaitMs = 800;

unsigned long lastPublishTime = 0;
unsigned long lastTempRequestTime = 0;
bool tempConversionInProgress = false;

// ======================================================
// STATE
// ======================================================
uint32_t readingIndex = 1;
float latestTempC = NAN;

// ======================================================
// HELPERS
// ======================================================
const char* anomalyModeToString(AnomalyMode mode) {
  switch (mode) {
    case ANOM_NONE: return "none";
    case ANOM_TEMP_ONLY: return "temp_only";
    case ANOM_VIB_ONLY: return "vib_only";
    case ANOM_BOTH: return "both";
    default: return "unknown";
  }
}

AnomalyMode scheduledAnomalyMode(uint32_t idx) {
  if (!FORCE_ANOMALY) return ANOM_NONE;

  for (size_t i = 0; i < ANOMALY_SCHEDULE_COUNT; i++) {
    uint32_t startIdx = ANOMALY_SCHEDULE[i].startIndex;
    uint32_t endIdx = startIdx + ANOMALY_SCHEDULE[i].count;

    if (idx >= startIdx && idx < endIdx) {
      return ANOMALY_SCHEDULE[i].mode;
    }
  }

  return ANOM_NONE;
}

void printAnomalySchedule() {
  Serial.println("Anomaly schedule:");
  for (size_t i = 0; i < ANOMALY_SCHEDULE_COUNT; i++) {
    uint32_t startIdx = ANOMALY_SCHEDULE[i].startIndex;
    uint32_t endIdx = startIdx + ANOMALY_SCHEDULE[i].count - 1;

    Serial.print("  Segment ");
    Serial.print(i + 1);
    Serial.print(": ");
    Serial.print(anomalyModeToString(ANOMALY_SCHEDULE[i].mode));
    Serial.print(" | start=");
    Serial.print(startIdx);
    Serial.print(" | end=");
    Serial.print(endIdx);
    Serial.print(" | count=");
    Serial.println(ANOMALY_SCHEDULE[i].count);
  }
}

void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  Serial.print("Connecting to WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println();
  Serial.print("Connected. IP: ");
  Serial.println(WiFi.localIP());
}

void setupOTA() {
  ArduinoOTA.setHostname("esp32-temp-vibration-002");

  ArduinoOTA.onStart([]() {
    Serial.println("OTA update started");
  });

  ArduinoOTA.onEnd([]() {
    Serial.println("\nOTA update finished");
  });

  ArduinoOTA.onError([](ota_error_t error) {
    Serial.printf("OTA Error[%u]\n", error);
  });

  ArduinoOTA.begin();
  Serial.println("OTA ready");
}

void setupMQTT() {
  mqttClient.setServer(mqttServer, mqttPort);
  mqttClient.setBufferSize(512);
}

void reconnectMQTT() {
  while (!mqttClient.connected()) {
    Serial.print("Connecting to MQTT... ");

    if (mqttClient.connect(mqttClientId)) {
      Serial.println("connected");
    } else {
      Serial.print("failed, rc=");
      Serial.print(mqttClient.state());
      Serial.println(" retrying in 2 seconds...");
      delay(2000);
    }
  }
}

// ======================================================
// ADXL345 LOW-LEVEL FUNCTIONS
// ======================================================
bool writeRegister(uint8_t reg, uint8_t value) {
  Wire.beginTransmission(ADXL345_ADDR);
  Wire.write(reg);
  Wire.write(value);
  return (Wire.endTransmission() == 0);
}

bool readRegister(uint8_t reg, uint8_t &value) {
  Wire.beginTransmission(ADXL345_ADDR);
  Wire.write(reg);
  if (Wire.endTransmission(false) != 0) {
    return false;
  }

  uint8_t received = Wire.requestFrom((uint8_t)ADXL345_ADDR, (uint8_t)1);
  if (received != 1) {
    return false;
  }

  value = Wire.read();
  return true;
}

bool readRegisters(uint8_t startReg, uint8_t count, uint8_t *data) {
  Wire.beginTransmission(ADXL345_ADDR);
  Wire.write(startReg);
  if (Wire.endTransmission(false) != 0) {
    return false;
  }

  uint8_t received = Wire.requestFrom((uint8_t)ADXL345_ADDR, count);
  if (received != count) {
    return false;
  }

  for (uint8_t i = 0; i < count; i++) {
    data[i] = Wire.read();
  }

  return true;
}

bool setupADXL345() {
  uint8_t devid = 0;
  if (!readRegister(ADXL345_DEVID_REG, devid)) {
    Serial.println("ERROR: Failed to read ADXL345 DEVID");
    return false;
  }

  Serial.print("ADXL345 DEVID = 0x");
  Serial.println(devid, HEX);

  if (devid != 0xE5) {
    Serial.println("ERROR: Unexpected ADXL345 device ID");
    return false;
  }

  if (!writeRegister(ADXL345_POWER_CTL, 0x00)) return false;
  if (!writeRegister(ADXL345_DATA_FORMAT, 0x08)) return false;
  if (!writeRegister(ADXL345_BW_RATE, 0x0A)) return false;
  if (!writeRegister(ADXL345_POWER_CTL, 0x08)) return false;

  delay(100);
  return true;
}

bool readADXL345(int16_t &x, int16_t &y, int16_t &z) {
  uint8_t rawData[6] = {0};

  if (!readRegisters(ADXL345_DATAX0, 6, rawData)) {
    return false;
  }

  x = (int16_t)((rawData[1] << 8) | rawData[0]);
  y = (int16_t)((rawData[3] << 8) | rawData[2]);
  z = (int16_t)((rawData[5] << 8) | rawData[4]);

  return true;
}

bool readSimulatedADXL345(int16_t &x, int16_t &y, int16_t &z) {
  float idx = (float)readingIndex;

  float x_g = 0.015f * sinf(idx * 0.07f);
  float y_g = 0.012f * cosf(idx * 0.05f);
  float z_g = SIM_Z_BASE_G + 0.008f * sinf(idx * 0.03f);

  x = (int16_t)lroundf(x_g / ADXL345_SCALE_G_PER_LSB);
  y = (int16_t)lroundf(y_g / ADXL345_SCALE_G_PER_LSB);
  z = (int16_t)lroundf(z_g / ADXL345_SCALE_G_PER_LSB);

  return true;
}

// ======================================================
// DS18B20 HELPERS
// ======================================================
void setupDS18B20() {
  pinMode(ONE_WIRE_BUS, INPUT_PULLUP);
  tempSensors.begin();
  tempSensors.setWaitForConversion(false);

  int deviceCount = tempSensors.getDeviceCount();
  Serial.print("DS18B20 device count = ");
  Serial.println(deviceCount);

  if (deviceCount == 0) {
    Serial.println("WARNING: No DS18B20 detected");
  }

  tempSensors.requestTemperatures();
  lastTempRequestTime = millis();
  tempConversionInProgress = true;
}

void updateTemperatureAsync() {
  if (SIMULATE_SENSORS) {
    float idx = (float)readingIndex;
    latestTempC = SIM_TEMP_BASE_C + 0.25f * sinf(idx * 0.04f);
    return;
  }

  unsigned long now = millis();

  if (tempConversionInProgress) {
    if (now - lastTempRequestTime >= tempConversionWaitMs) {
      float t = tempSensors.getTempCByIndex(0);
      if (t != DEVICE_DISCONNECTED_C) {
        latestTempC = t;
      } else {
        Serial.println("WARNING: DS18B20 disconnected or invalid reading");
      }

      tempSensors.requestTemperatures();
      lastTempRequestTime = now;
    }
  } else {
    tempSensors.requestTemperatures();
    lastTempRequestTime = now;
    tempConversionInProgress = true;
  }
}

// ======================================================
// SETUP
// ======================================================
void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println();
  Serial.println("Starting system...");

  if (SIMULATE_SENSORS) {
    Serial.println("SIMULATION MODE: skipping physical sensor initialization");
    latestTempC = SIM_TEMP_BASE_C;
  } else {
    setupDS18B20();

    Wire.begin(21, 22);
    delay(200);

    if (!setupADXL345()) {
      Serial.println("FATAL: ADXL345 setup failed");
    } else {
      Serial.println("ADXL345 initialized successfully");
    }
  }

  connectWiFi();
  setupOTA();
  setupMQTT();

  Serial.println("System ready");
  Serial.println("------------------------------------");
  Serial.print("SIMULATE_SENSORS = ");
  Serial.println(SIMULATE_SENSORS ? "true" : "false");
  Serial.print("FORCE_ANOMALY = ");
  Serial.println(FORCE_ANOMALY ? "true" : "false");
  printAnomalySchedule();
  Serial.println("------------------------------------");
}

// ======================================================
// LOOP
// ======================================================
void loop() {
  ArduinoOTA.handle();

  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("WiFi disconnected -> reconnecting");
    connectWiFi();
  }

  if (!mqttClient.connected()) {
    reconnectMQTT();
  }
  mqttClient.loop();

  updateTemperatureAsync();

  unsigned long now = millis();
  if (now - lastPublishTime >= publishIntervalMs) {
    lastPublishTime = now;

    int16_t x = 0, y = 0, z = 0;

    if (SIMULATE_SENSORS) {
      readSimulatedADXL345(x, y, z);
    } else {
      if (!readADXL345(x, y, z)) {
        Serial.println("ERROR: Failed to read ADXL345 data");
        return;
      }
    }

    if (!SIMULATE_SENSORS && (isnan(latestTempC) || latestTempC == DEVICE_DISCONNECTED_C)) {
      return;
    }

    int16_t rawXToSend = x;
    int16_t rawYToSend = y;
    int16_t rawZToSend = z;

    float x_g = x * ADXL345_SCALE_G_PER_LSB;
    float y_g = y * ADXL345_SCALE_G_PER_LSB;
    float z_g = z * ADXL345_SCALE_G_PER_LSB;
    float tempToSend = latestTempC;

    AnomalyMode modeNow = scheduledAnomalyMode(readingIndex);
    bool anomalyNow = (modeNow != ANOM_NONE);

    if (anomalyNow) {
      if (modeNow == ANOM_TEMP_ONLY || modeNow == ANOM_BOTH) {
        tempToSend = FORCED_TEMP_C;
      }

      if (modeNow == ANOM_VIB_ONLY || modeNow == ANOM_BOTH) {
        x_g = FORCED_X_G;
        y_g = FORCED_Y_G;
        z_g = FORCED_Z_G;

        rawXToSend = (int16_t)lroundf(FORCED_X_G / ADXL345_SCALE_G_PER_LSB);
        rawYToSend = (int16_t)lroundf(FORCED_Y_G / ADXL345_SCALE_G_PER_LSB);
        rawZToSend = (int16_t)lroundf(FORCED_Z_G / ADXL345_SCALE_G_PER_LSB);
      }

      Serial.print(">>> FORCED ANOMALY ACTIVE: ");
      Serial.print(anomalyModeToString(modeNow));
      Serial.println(" <<<");
    }

    char payload[384];
    snprintf(
      payload,
      sizeof(payload),
      "{\"factoryId\":\"%s\",\"machineId\":\"%s\",\"deviceId\":\"%s\",\"readingIndex\":%lu,\"temperatureC\":%.2f,\"rawX\":%d,\"rawY\":%d,\"rawZ\":%d,\"x_g\":%.3f,\"y_g\":%.3f,\"z_g\":%.3f,\"mode\":\"%s\"}",
      factoryId,
      machineId,
      deviceId,
      (unsigned long)readingIndex,
      tempToSend,
      rawXToSend,
      rawYToSend,
      rawZToSend,
      x_g,
      y_g,
      z_g,
      anomalyNow ? anomalyModeToString(modeNow) : "normal"
    );

    bool published = mqttClient.publish(mqttTopic, payload, false);

    if (published) {
      Serial.print("Published reading ");
      Serial.print(readingIndex);
      Serial.print(" | TempC=");
      Serial.print(tempToSend, 2);
      Serial.print(" | Xg=");
      Serial.print(x_g, 3);
      Serial.print(" | Yg=");
      Serial.print(y_g, 3);
      Serial.print(" | Zg=");
      Serial.print(z_g, 3);
      Serial.print(" | Mode=");
      Serial.println(anomalyNow ? anomalyModeToString(modeNow) : "normal");
    } else {
      Serial.print("MQTT publish FAILED | WiFi.status=");
      Serial.print(WiFi.status());
      Serial.print(" | mqtt.connected=");
      Serial.print(mqttClient.connected() ? "true" : "false");
      Serial.print(" | mqtt.state=");
      Serial.print(mqttClient.state());
      Serial.print(" | heap=");
      Serial.println(ESP.getFreeHeap());
    }

    readingIndex++;
  }
}