#include <WiFi.h>
#include <ArduinoOTA.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h>
#include <PubSubClient.h>

// =========================
// WiFi CONFIG
// =========================
const char* ssid = "Klea";
const char* password = "rinesanart";

// =========================
// MQTT CONFIG
// =========================
const char* mqttServer = "192.168.178.142";   // <-- PUT YOUR PI4 IP HERE
const int mqttPort = 1883;
const char* mqttTopic = "factory/factory-001/machine/machine-001/telemetry";
const char* mqttClientId = "esp32-001";

// =========================
// IDs
// =========================
const char* factoryId = "factory-001";
const char* machineId = "machine-001";
const char* deviceId = "esp32-001";

// =========================
// DS18B20 Temperature Sensor
// =========================
#define ONE_WIRE_BUS 4

OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature tempSensors(&oneWire);

// =========================
// ADXL345 Vibration Sensor
// =========================
#define ADXL345_ADDR 0x53

// =========================
// WiFi / MQTT objects
// =========================
WiFiClient espClient;
PubSubClient mqttClient(espClient);

int readingIndex = 1;
unsigned long lastPublishTime = 0;
const unsigned long publishIntervalMs = 2000; // publish every 2 seconds

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

// -------------------------
// WiFi setup
// -------------------------
void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  Serial.print("Connecting to WiFi");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }

  Serial.println();
  Serial.print("Connected! IP: ");
  Serial.println(WiFi.localIP());
}

// -------------------------
// OTA setup
// -------------------------
void setupOTA() {
  ArduinoOTA.setHostname("esp32-temp-vibration");

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

// -------------------------
// MQTT setup
// -------------------------
void setupMQTT() {
  mqttClient.setServer(mqttServer, mqttPort);
}

// -------------------------
// MQTT reconnect
// -------------------------
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

// =========================
// SETUP
// =========================
void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println("Starting system...");

  // ---- DS18B20 FIRST ----
  pinMode(ONE_WIRE_BUS, INPUT_PULLUP);
  tempSensors.begin();

  int deviceCount = tempSensors.getDeviceCount();
  Serial.print("DS18B20 device count = ");
  Serial.println(deviceCount);

  if (deviceCount == 0) {
    Serial.println("WARNING: No DS18B20 detected!");
  }

  // ---- ADXL345 ----
  Wire.begin(21, 22);
  delay(500);

  writeRegister(0x2D, 0x08); // measurement mode
  writeRegister(0x31, 0x08); // full resolution

  Wire.beginTransmission(ADXL345_ADDR);
  if (Wire.endTransmission() == 0) {
    Serial.println("ADXL345 OK");
  } else {
    Serial.println("ADXL345 NOT detected!");
  }

  // ---- WiFi + OTA + MQTT ----
  connectWiFi();
  setupOTA();
  setupMQTT();

  Serial.println("System ready.");
  Serial.println("------------------------------------");
}

// =========================
// LOOP
// =========================
void loop() {
  ArduinoOTA.handle();

  if (WiFi.status() != WL_CONNECTED) {
    connectWiFi();
  }

  if (!mqttClient.connected()) {
    reconnectMQTT();
  }
  mqttClient.loop();

  unsigned long currentMillis = millis();
  if (currentMillis - lastPublishTime >= publishIntervalMs) {
    lastPublishTime = currentMillis;

    // =========================
    // Temperature
    // =========================
    tempSensors.requestTemperatures();
    delay(100);

    float tempC = tempSensors.getTempCByIndex(0);

    // =========================
    // Vibration
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
    // Serial debug
    // =========================
    Serial.print("Reading ");
    Serial.println(readingIndex);

    if (tempC == DEVICE_DISCONNECTED_C) {
      Serial.println("ERROR: DS18B20 disconnected!");
    } else {
      Serial.print("Temperature (C): ");
      Serial.println(tempC);
    }

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

    // =========================
    // Build JSON payload
    // =========================
    char payload[256];

    snprintf(
      payload,
      sizeof(payload),
      "{\"factoryId\":\"%s\",\"machineId\":\"%s\",\"deviceId\":\"%s\",\"readingIndex\":%d,\"temperatureC\":%.2f,\"rawX\":%d,\"rawY\":%d,\"rawZ\":%d,\"x_g\":%.3f,\"y_g\":%.3f,\"z_g\":%.3f}",
      factoryId,
      machineId,
      deviceId,
      readingIndex,
      tempC,
      x,
      y,
      z,
      x_g,
      y_g,
      z_g
    );

    // =========================
    // Publish to MQTT
    // =========================
    bool published = mqttClient.publish(mqttTopic, payload);

    if (published) {
      Serial.println("MQTT publish OK");
      Serial.println(payload);
    } else {
      Serial.println("MQTT publish FAILED");
    }

    Serial.println("------------------------------------");

    readingIndex++;
  }
}