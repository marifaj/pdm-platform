void setup() {
  Serial.begin(115200);

  // Optional: small startup delay for stable serial connection
  delay(200);
  Serial.println("ESP32 started.");
}

void loop() {
  static unsigned long counter = 1;

  Serial.println("ESP32 is working!");
  Serial.print("Counter: ");
  Serial.println(counter);

  counter++;
  delay(1000);
}