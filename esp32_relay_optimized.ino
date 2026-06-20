// ============================================================
//  IoT ESP32 — Kontrol 4 Relay + Sensor DHT11 + Firebase
//  OPTIMIZED: Streaming untuk relay (0 delay), sensor polling terpisah
//  Library: Firebase ESP Client (Mobizt), DHT (Adafruit)
// ============================================================

#include <WiFi.h>
#include <Firebase_ESP_Client.h>
#include <DHT.h>
#include "addons/TokenHelper.h"
#include "addons/RTDBHelper.h"

// ============================================================
//  KONFIGURASI
// ============================================================
#define WIFI_SSID      "iPhone"
#define WIFI_PASSWORD  "rendyy123"

#define API_KEY        "AIzaSyCQdXGar5NCtExlMH8iJ7X2-BzhaJKRMeU"
#define DATABASE_URL   "https://memey-iot-firebase-default-rtdb.asia-southeast1.firebasedatabase.app"
#define DEVICE_PATH    "/devices/esp32_01"

// ============================================================
//  PIN MAPPING
// ============================================================
#define RELAY1_PIN  23
#define RELAY2_PIN  19
#define RELAY3_PIN  18
#define RELAY4_PIN  5
#define DHT_PIN     4
#define DHT_TYPE    DHT11

#define RELAY_ON    LOW
#define RELAY_OFF   HIGH

// ============================================================
//  INTERVAL
// ============================================================
#define SENSOR_INTERVAL    5000   // Kirim suhu/kelembapan tiap 5 detik
#define ANIMASI1_INTERVAL  400    // ms per step (running light)
#define ANIMASI2_INTERVAL  500    // ms per blink (blink all)
#define WIFI_CHECK_INTERVAL 10000 // Cek koneksi WiFi tiap 10 detik

// ============================================================
//  OBJEK FIREBASE — DIPISAH agar tidak saling blok
//  fbdoStream : khusus streaming relay + mode (real-time)
//  fbdoSensor : khusus kirim data sensor (polling)
// ============================================================
FirebaseData   fbdoStream;   // Untuk streaming (relay + mode)
FirebaseData   fbdoSensor;   // Untuk sensor write
FirebaseAuth   auth;
FirebaseConfig config;

DHT dht(DHT_PIN, DHT_TYPE);

// ============================================================
//  STATE
// ============================================================
bool relayState[4] = {false, false, false, false};
int  animasiMode   = 0;

unsigned long lastSensorTime   = 0;
unsigned long lastAnimasiTime  = 0;
unsigned long lastWiFiCheck    = 0;

int  animasiStep  = 0;
bool animasiBlink = false;

// Flag: streaming sudah aktif?
bool streamingActive = false;

// ============================================================
//  PROTOTYPES
// ============================================================
void connectWiFi();
void initFirebase();
void startStreaming();
void streamCallback(FirebaseStream data);
void streamTimeoutCallback(bool timeout);
void bacaSensor();
void setRelay(int index, bool state);
void setSemuaRelay(bool state);
void jalankanVariasi1();
void jalankanVariasi2();
void updateRelayToFirebase(int index, bool state);

// ============================================================
//  SETUP
// ============================================================
void setup() {
  Serial.begin(115200);
  delay(500);

  // Inisialisasi pin relay
  int pins[4] = {RELAY1_PIN, RELAY2_PIN, RELAY3_PIN, RELAY4_PIN};
  for (int i = 0; i < 4; i++) {
    pinMode(pins[i], OUTPUT);
  }
  setSemuaRelay(false);
  Serial.println("[RELAY] Semua relay OFF (startup)");

  dht.begin();
  Serial.println("[DHT] Sensor DHT11 siap");

  connectWiFi();
  initFirebase();

  // Set status online
  Firebase.RTDB.setBool(&fbdoSensor, DEVICE_PATH "/status/online", true);

  // Mulai streaming — ini yang menggantikan polling relay
  startStreaming();

  Serial.println("[SYSTEM] Sistem siap!");
}

// ============================================================
//  LOOP UTAMA
//  Catatan: tidak ada lagi polling relay di sini.
//  Relay dikontrol via callback streamCallback() secara real-time.
// ============================================================
void loop() {
  if (!Firebase.ready()) return;

  unsigned long now = millis();

  // --- 1. Kirim data sensor ---
  if (now - lastSensorTime >= SENSOR_INTERVAL) {
    lastSensorTime = now;
    bacaSensor();
  }

  // --- 2. Jalankan animasi jika aktif ---
  if (animasiMode == 1) {
    jalankanVariasi1();
  } else if (animasiMode == 2) {
    jalankanVariasi2();
  }

  // --- 3. Reconnect WiFi jika putus ---
  if (now - lastWiFiCheck >= WIFI_CHECK_INTERVAL) {
    lastWiFiCheck = now;
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[WiFi] Koneksi terputus, reconnect...");
      connectWiFi();
      startStreaming(); // Restart streaming setelah reconnect
    }
  }
}

// ============================================================
//  KONEKSI WiFi
// ============================================================
void connectWiFi() {
  Serial.printf("[WiFi] Menghubungkan ke %s", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  int attempt = 0;
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    if (++attempt > 30) {
      Serial.println("\n[WiFi] GAGAL! Restart...");
      ESP.restart();
    }
  }
  Serial.printf("\n[WiFi] Terhubung! IP: %s\n", WiFi.localIP().toString().c_str());
}

// ============================================================
//  INISIALISASI FIREBASE
// ============================================================
void initFirebase() {
  config.api_key      = API_KEY;
  config.database_url = DATABASE_URL;

  if (Firebase.signUp(&config, &auth, "", "")) {
    Serial.println("[Firebase] Sign-up anonim berhasil");
  } else {
    Serial.printf("[Firebase] ERROR: %s\n", config.signer.signupError.message.c_str());
  }

  config.token_status_callback = tokenStatusCallback;

  // Ukuran buffer stream — naikkan agar tidak overflow saat terima JSON besar
  fbdoStream.setBSSLBufferSize(2048, 1024);
  fbdoSensor.setBSSLBufferSize(2048, 1024);

  Firebase.begin(&config, &auth);
  Firebase.reconnectWiFi(true);

  Serial.println("[Firebase] Firebase diinisialisasi");
}

// ============================================================
//  MULAI STREAMING — Listen ke /control dan /mode sekaligus
//
//  Firebase streaming bekerja seperti WebSocket:
//  Setiap ada perubahan di node yang di-listen, server langsung
//  push data ke ESP32 tanpa perlu polling.
//  Hasilnya: delay relay ≈ network latency saja (~50-200ms)
//  bukan polling interval (200ms + latency).
// ============================================================
void startStreaming() {
  // Stream ke parent node agar relay1-4 dan mode terpantau sekaligus
  // dengan 1 koneksi streaming saja
  if (!Firebase.RTDB.beginStream(&fbdoStream, DEVICE_PATH)) {
    Serial.printf("[Stream] ERROR begin: %s\n", fbdoStream.errorReason().c_str());
    return;
  }

  Firebase.RTDB.setStreamCallback(&fbdoStream, streamCallback, streamTimeoutCallback);
  streamingActive = true;
  Serial.println("[Stream] Streaming aktif di: " DEVICE_PATH);
}

// ============================================================
//  STREAM CALLBACK — Dipanggil otomatis saat ada perubahan di Firebase
//  Ini inti dari solusi delay: tidak perlu polling, server push langsung.
// ============================================================
void streamCallback(FirebaseStream data) {
  String path = data.dataPath();
  Serial.printf("[Stream] Update di: %s | Tipe: %s\n",
                path.c_str(), data.dataType().c_str());

  // ── Perubahan individual relay ──────────────────────────
  // Path contoh: /control/relay1
  if (path.startsWith("/control/relay")) {
    // Ambil index relay dari path (relay1→0, relay2→1, dst)
    char lastChar = path.charAt(path.length() - 1);
    int idx = (int)(lastChar - '1'); // '1'=0x31, '2'=0x32, dst

    if (idx >= 0 && idx < 4 && data.dataType() == "boolean") {
      bool newState = data.boolData();
      if (newState != relayState[idx]) {
        setRelay(idx, newState);
        Serial.printf("[RELAY] Relay %d → %s\n", idx + 1, newState ? "ON" : "OFF");
      }
    }
  }

  // ── Update semua control sekaligus (misal dari JSON write) ─
  // Path: /control
  else if (path == "/control") {
    if (data.dataType() == "json") {
      FirebaseJson json;
      json.setJsonData(data.stringData());
      FirebaseJsonData result;

      for (int i = 0; i < 4; i++) {
        String key = "relay" + String(i + 1);
        json.get(result, key);
        if (result.success) {
          bool newState = result.boolValue;
          if (newState != relayState[i]) {
            setRelay(i, newState);
            Serial.printf("[RELAY] Relay %d → %s\n", i + 1, newState ? "ON" : "OFF");
          }
        }
      }
    }
  }

  // ── Mode animasi berubah ────────────────────────────────
  // Path: /mode/animasi
  else if (path == "/mode/animasi") {
    if (data.dataType() == "int" || data.dataType() == "number") {
      int newMode = data.intData();
      if (newMode != animasiMode) {
        animasiMode  = newMode;
        animasiStep  = 0;
        animasiBlink = false;
        setSemuaRelay(false);
        Serial.printf("[MODE] Animasi mode → %d\n", animasiMode);
      }
    }
  }

  // ── Root node: ESP32 baru connect, terima snapshot awal ─
  // Ini terjadi saat pertama kali stream dimulai
  else if (path == "/" && data.dataType() == "json") {
    Serial.println("[Stream] Snapshot awal diterima, sinkronisasi state...");
    FirebaseJson json;
    json.setJsonData(data.stringData());
    FirebaseJsonData result;

    // Sinkronisasi relay
    for (int i = 0; i < 4; i++) {
      String key = "control/relay" + String(i + 1);
      json.get(result, key);
      if (result.success) {
        bool st = result.boolValue;
        setRelay(i, st);
      }
    }

    // Sinkronisasi mode animasi
    json.get(result, "mode/animasi");
    if (result.success) {
      animasiMode  = result.intValue;
      animasiStep  = 0;
      animasiBlink = false;
      Serial.printf("[MODE] Mode awal dari Firebase: %d\n", animasiMode);
    }
  }
}

// ============================================================
//  STREAM TIMEOUT CALLBACK
//  Dipanggil jika koneksi stream timeout/terputus
//  Firebase ESP Client akan otomatis reconnect, tapi kita log dulu
// ============================================================
void streamTimeoutCallback(bool timeout) {
  if (timeout) {
    Serial.println("[Stream] TIMEOUT — Firebase akan reconnect otomatis...");
  }
}

// ============================================================
//  BACA DAN KIRIM DATA SENSOR DHT11
// ============================================================
void bacaSensor() {
  float suhu       = dht.readTemperature();
  float kelembapan = dht.readHumidity();

  if (isnan(suhu) || isnan(kelembapan)) {
    Serial.println("[DHT] ERROR: Gagal membaca sensor!");
    return;
  }

  Serial.printf("[DHT] Suhu: %.1f°C | Kelembapan: %.1f%%\n", suhu, kelembapan);

  // Kirim pakai fbdoSensor — TIDAK mengganggu fbdoStream
  if (Firebase.RTDB.setFloat(&fbdoSensor, DEVICE_PATH "/sensor/temperature", suhu)) {
    Serial.println("[FB] temperature OK");
  } else {
    Serial.printf("[FB] temperature GAGAL: %s\n", fbdoSensor.errorReason().c_str());
  }

  if (Firebase.RTDB.setFloat(&fbdoSensor, DEVICE_PATH "/sensor/humidity", kelembapan)) {
    Serial.println("[FB] humidity OK");
  } else {
    Serial.printf("[FB] humidity GAGAL: %s\n", fbdoSensor.errorReason().c_str());
  }

  Firebase.RTDB.setInt(&fbdoSensor, DEVICE_PATH "/sensor/last_update", (int)(millis() / 1000));
}

// ============================================================
//  SET RELAY FISIK
// ============================================================
void setRelay(int index, bool state) {
  int pins[4] = {RELAY1_PIN, RELAY2_PIN, RELAY3_PIN, RELAY4_PIN};
  if (index < 0 || index > 3) return;
  digitalWrite(pins[index], state ? RELAY_ON : RELAY_OFF);
  relayState[index] = state;
}

// ============================================================
//  SET SEMUA RELAY
// ============================================================
void setSemuaRelay(bool state) {
  for (int i = 0; i < 4; i++) {
    setRelay(i, state);
  }
}

// ============================================================
//  VARIASI 1 — Running Light
// ============================================================
void jalankanVariasi1() {
  unsigned long now = millis();
  if (now - lastAnimasiTime < ANIMASI1_INTERVAL) return;
  lastAnimasiTime = now;

  setSemuaRelay(false);
  setRelay(animasiStep, true);
  Serial.printf("[ANIM1] Relay %d ON\n", animasiStep + 1);
  animasiStep = (animasiStep + 1) % 4;
}

// ============================================================
//  VARIASI 2 — Blink All
// ============================================================
void jalankanVariasi2() {
  unsigned long now = millis();
  if (now - lastAnimasiTime < ANIMASI2_INTERVAL) return;
  lastAnimasiTime = now;

  animasiBlink = !animasiBlink;
  setSemuaRelay(animasiBlink);
  Serial.printf("[ANIM2] Semua relay → %s\n", animasiBlink ? "ON" : "OFF");
}

// ============================================================
//  UPDATE STATE RELAY KE FIREBASE (sinkronisasi balik)
//  Gunakan ini jika relay diubah oleh tombol fisik lokal
// ============================================================
void updateRelayToFirebase(int index, bool state) {
  if (index < 0 || index > 3) return;
  const char* relayPath[4] = {
    DEVICE_PATH "/control/relay1",
    DEVICE_PATH "/control/relay2",
    DEVICE_PATH "/control/relay3",
    DEVICE_PATH "/control/relay4",
  };
  Firebase.RTDB.setBool(&fbdoSensor, relayPath[index], state);
}
