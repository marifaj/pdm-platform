#include <WiFi.h>
#include <ArduinoOTA.h>
#include <PubSubClient.h>
#include <math.h>

// ======================================================
// DEVICE 2 - SIMULATED TELEMETRY NODE
// ======================================================
// Purpose:
// - Use this on ESP32-002 when the second DS18B20/ADXL345 hardware
//   is not available or not reliable.
// - It publishes synthetic temperature + vibration telemetry at the
//   same nominal rate as a real node.
// - This is suitable for IoT workload/scalability testing: throughput,
//   MQTT reliability, latency, and Raspberry Pi 4 resource usage.
//
// Important paper wording:
// - Describe this as an "emulated/synthetic ESP32 telemetry node", not
//   as a second physical sensor node.
//
// MQTT note:
// - PubSubClient publishes with MQTT QoS 0. Reliability is evaluated
//   through readingIndex sequence gaps and publish-failure counters.

// ======================================================
// EXPERIMENT CONFIG - CHANGE ONLY THESE FOR EACH RUN
// ======================================================
const char* runId = "S6_multi_device_100hz_normal_esp32_002_sim";

// Use 50 for S5, then 100 for S6 if stable.
const int TARGET_HZ = 100;

// Keep false for normal runs. Set true only for the anomaly run on device 002.
bool FORCE_ANOMALY = false;

// Identifies the source as simulated in SQL/logs.
const char* dataSource = "simulated";
const char* sensorMode = "synthetic";

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
// SIMULATED NORMAL SIGNAL CONFIG
// ======================================================
// These values roughly mimic a quiet motor/accelerometer stream. Adjust only
// if you want the synthetic stream to look closer to your real device-001 data.
const float NORMAL_TEMP_BASE_C = 23.50f;
const float NORMAL_X_BASE_G = -0.043f;
const float NORMAL_Y_BASE_G = -0.008f;
const float NORMAL_Z_BASE_G = -1.077f;

// Small random noise amplitudes.
const float TEMP_NOISE_C = 0.08f;   // +/- 0.08 C
const float VIB_NOISE_G = 0.010f;   // +/- 0.010 g

// Slow temperature drift amplitude for more natural synthetic data.
const float TEMP_DRIFT_C = 0.20f;
const float TEMP_DRIFT_PERIOD_MS = 180000.0f; // 3 minutes

// ADXL345-compatible raw conversion assumption for payload consistency.
const float ADXL345_SCALE_G_PER_LSB = 0.0039f;

// ======================================================
// ANOMALY SIMULATION CONFIG
// ======================================================
// The schedule is time-based through TARGET_HZ, so it keeps the same
// minutes/seconds whether you run at 50 Hz or 100 Hz.
//
// 00:00-05:00 normal
// 05:00-08:00 temperature-only anomaly
// 08:00-11:00 recovery
// 11:00-14:00 vibration-only anomaly
// 14:00-17:00 recovery
// 17:00-20:00 temperature + vibration anomaly
// 20:00+      recovery/final normal

#define SEC_TO_INDEX(sec) ((uint32_t)((sec) * TARGET_HZ) + 1UL)
#define SEC_TO_COUNT(sec) ((uint32_t)((sec) * TARGET_HZ))

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
  {SEC_TO_INDEX(300),  SEC_TO_COUNT(180), ANOM_TEMP_ONLY}, // 05:00-08:00
  {SEC_TO_INDEX(660),  SEC_TO_COUNT(180), ANOM_VIB_ONLY},  // 11:00-14:00
  {SEC_TO_INDEX(1020), SEC_TO_COUNT(180), ANOM_BOTH}       // 17:00-20:00
};

const size_t ANOMALY_SCHEDULE_COUNT =
  sizeof(ANOMALY_SCHEDULE) / sizeof(ANOMALY_SCHEDULE[0]);

// Forced anomaly values. These match the previous controlled-anomaly design.
const float FORCED_TEMP_C = 38.0f;
const float FORCED_X_G = 0.90f;
const float FORCED_Y_G = 0.85f;
const float FORCED_Z_G = 0.20f;

// ======================================================
// WIFI / MQTT OBJECTS
// ======================================================
WiFiClient espClient;
PubSubClient mqttClient(espClient);

// ======================================================
// TIMING
// ======================================================
const unsigned long publishIntervalMs = 1000UL / TARGET_HZ;
const unsigned long statusPrintIntervalMs = 10000; // one status line every 10s

unsigned long lastPublishTime = 0;
unsigned long lastStatusPrintTime = 0;

// ======================================================
// STATE / COUNTERS
// ======================================================
uint32_t readingIndex = 1;

uint32_t mqttPublishOk = 0;
uint32_t mqttPublishFailed = 0;
uint32_t wifiReconnectCount = 0;
uint32_t mqttReconnectAttemptCount = 0;
uint32_t mqttReconnectSuccessCount = 0;

// ======================================================
// HELPERS
// ======================================================
const char* anomalyModeToString(AnomalyMode mode) {
  switch (mode) {
    case ANOM_NONE:      return "none";
    case ANOM_TEMP_ONLY: return "temp_only";
    case ANOM_VIB_ONLY:  return "vib_only";
    case ANOM_BOTH:      return "both";
    default:             return "unknown";
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

float randomFloat(float minValue, float maxValue) {
  long r = random(0, 10001); // 0..10000
  float ratio = ((float)r) / 10000.0f;
  return minValue + (maxValue - minValue) * ratio;
}

float tempDrift(unsigned long nowMs) {
  float angle = (2.0f * PI * ((float)(nowMs % (unsigned long)TEMP_DRIFT_PERIOD_MS))) / TEMP_DRIFT_PERIOD_MS;
  return TEMP_DRIFT_C * sinf(angle);
}

void generateSyntheticNormal(float &tempC, float &xg, float &yg, float &zg) {
  unsigned long nowMs = millis();

  tempC = NORMAL_TEMP_BASE_C
          + tempDrift(nowMs)
          + randomFloat(-TEMP_NOISE_C, TEMP_NOISE_C);

  xg = NORMAL_X_BASE_G + randomFloat(-VIB_NOISE_G, VIB_NOISE_G);
  yg = NORMAL_Y_BASE_G + randomFloat(-VIB_NOISE_G, VIB_NOISE_G);
  zg = NORMAL_Z_BASE_G + randomFloat(-VIB_NOISE_G, VIB_NOISE_G);
}

int16_t gToRaw(float g) {
  return (int16_t)lroundf(g / ADXL345_SCALE_G_PER_LSB);
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
    Serial.print(" | startIndex=");
    Serial.print(startIdx);
    Serial.print(" | endIndex=");
    Serial.print(endIdx);
    Serial.print(" | count=");
    Serial.println(ANOMALY_SCHEDULE[i].count);
  }
}

void printRunConfig() {
  Serial.println("------------------------------------");
  Serial.println("RUN CONFIG - SIMULATED DEVICE 2");
  Serial.print("runId = ");
  Serial.println(runId);
  Serial.print("deviceId = ");
  Serial.println(deviceId);
  Serial.print("dataSource = ");
  Serial.println(dataSource);
  Serial.print("sensorMode = ");
  Serial.println(sensorMode);
  Serial.print("mqttClientId = ");
  Serial.println(mqttClientId);
  Serial.print("mqttTopic = ");
  Serial.println(mqttTopic);
  Serial.print("TARGET_HZ = ");
  Serial.println(TARGET_HZ);
  Serial.print("publishIntervalMs = ");
  Serial.println(publishIntervalMs);
  Serial.print("FORCE_ANOMALY = ");
  Serial.println(FORCE_ANOMALY ? "true" : "false");
  printAnomalySchedule();
  Serial.println("------------------------------------");
}

void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.persistent(false);
  WiFi.setAutoReconnect(true);
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
  ArduinoOTA.setHostname("esp32-simulated-telemetry-002");

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
  mqttClient.setBufferSize(768);
  mqttClient.setKeepAlive(20);
  mqttClient.setSocketTimeout(3);
}

void reconnectMQTT() {
  while (!mqttClient.connected()) {
    mqttReconnectAttemptCount++;
    Serial.print("Connecting to MQTT... ");

    if (mqttClient.connect(mqttClientId)) {
      mqttReconnectSuccessCount++;
      Serial.println("connected");
    } else {
      Serial.print("failed, rc=");
      Serial.print(mqttClient.state());
      Serial.println(" retrying in 2 seconds...");
      delay(2000);
    }
  }
}

void printStatusLine() {
  Serial.print("STATUS | runId=");
  Serial.print(runId);
  Serial.print(" | deviceId=");
  Serial.print(deviceId);
  Serial.print(" | dataSource=");
  Serial.print(dataSource);
  Serial.print(" | readingIndex=");
  Serial.print(readingIndex);
  Serial.print(" | ok=");
  Serial.print(mqttPublishOk);
  Serial.print(" | failed=");
  Serial.print(mqttPublishFailed);
  Serial.print(" | WiFiRSSI=");
  Serial.print(WiFi.RSSI());
  Serial.print(" | freeHeap=");
  Serial.print(ESP.getFreeHeap());
  Serial.print(" | wifiReconnects=");
  Serial.print(wifiReconnectCount);
  Serial.print(" | mqttReconnectAttempts=");
  Serial.print(mqttReconnectAttemptCount);
  Serial.print(" | mqttReconnectSuccess=");
  Serial.print(mqttReconnectSuccessCount);
  Serial.print(" | mqttState=");
  Serial.println(mqttClient.state());
}

// ======================================================
// SETUP
// ======================================================
void setup() {
  Serial.begin(115200);
  delay(1000);

  randomSeed((uint32_t)esp_random());

  Serial.println();
  Serial.println("Starting ESP32 simulated temperature + vibration telemetry node...");

  printRunConfig();

  connectWiFi();
  setupOTA();
  setupMQTT();

  Serial.println("System ready");
  Serial.println("------------------------------------");
}

// ======================================================
// LOOP
// ======================================================
void loop() {
  ArduinoOTA.handle();

  if (WiFi.status() != WL_CONNECTED) {
    wifiReconnectCount++;
    Serial.println("WiFi disconnected -> reconnecting");
    connectWiFi();
  }

  if (!mqttClient.connected()) {
    reconnectMQTT();
  }
  mqttClient.loop();

  unsigned long now = millis();

  if (now - lastStatusPrintTime >= statusPrintIntervalMs) {
    lastStatusPrintTime = now;
    printStatusLine();
  }

  if (now - lastPublishTime >= publishIntervalMs) {
    lastPublishTime = now;

    float tempToSend = NAN;
    float x_g = NAN;
    float y_g = NAN;
    float z_g = NAN;

    generateSyntheticNormal(tempToSend, x_g, y_g, z_g);

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
      }

      // Avoid printing every anomaly message because it can disturb 50/100 Hz publishing.
      if (readingIndex % (TARGET_HZ * 5) == 0) {
        Serial.print(">>> SYNTHETIC ANOMALY ACTIVE: ");
        Serial.print(anomalyModeToString(modeNow));
        Serial.println(" <<<");
      }
    }

    int16_t rawXToSend = gToRaw(x_g);
    int16_t rawYToSend = gToRaw(y_g);
    int16_t rawZToSend = gToRaw(z_g);

    char payload[768];
    snprintf(
      payload,
      sizeof(payload),
      "{\"runId\":\"%s\",\"targetHz\":%d,\"espMillis\":%lu,\"dataSource\":\"%s\",\"sensorMode\":\"%s\",\"factoryId\":\"%s\",\"machineId\":\"%s\",\"deviceId\":\"%s\",\"readingIndex\":%lu,\"temperatureC\":%.2f,\"rawX\":%d,\"rawY\":%d,\"rawZ\":%d,\"x_g\":%.3f,\"y_g\":%.3f,\"z_g\":%.3f,\"mode\":\"%s\"}",
      runId,
      TARGET_HZ,
      (unsigned long)now,
      dataSource,
      sensorMode,
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
      mqttPublishOk++;

      // Print only once every 10 seconds.
      if (readingIndex % (TARGET_HZ * 10) == 0) {
        Serial.print("Published reading ");
        Serial.print(readingIndex);
        Serial.print(" | mode=");
        Serial.print(anomalyNow ? anomalyModeToString(modeNow) : "normal");
        Serial.print(" | source=");
        Serial.print(dataSource);
        Serial.print(" | TempC=");
        Serial.print(tempToSend, 2);
        Serial.print(" | Xg=");
        Serial.print(x_g, 3);
        Serial.print(" | Yg=");
        Serial.print(y_g, 3);
        Serial.print(" | Zg=");
        Serial.println(z_g, 3);
      }
    } else {
      mqttPublishFailed++;
      Serial.print("MQTT publish FAILED | readingIndex=");
      Serial.print(readingIndex);
      Serial.print(" | WiFi.status=");
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
