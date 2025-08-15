# Crypto Chart Core Monolith

A cross-platform Flutter application that renders real-time crypto price charts using a **single, monolithic, pure-Dart core** running in a dedicated **Isolate**. The app connects directly to multiple exchange **WebSocket** and **REST** APIs, aggregates trades into time buckets (VWAP + volume), and displays candlesticks with VWAP and volume overlays.

---

## Highlights

- **Monolithic Core**: All business logic (networking, exchange adapters, normalization, aggregation, persistence, command handling) is implemented entirely in a single file: `lib/app_core.dart`. No Flutter imports in this file.
- **Strict Fixed-Point Math**: Prices, sizes, and P×V computed and stored as **signed 64-bit integers scaled by 1e8**.
- **Real-Time Aggregation**: 250ms cadence; per-bucket VWAP, volume, and last price; late-trade amend rules.
- **Exchanges Implemented**:
  - Binance (BTC/USDT)
  - OKX (BTC/USDT)
  - Bitget (BTC/USDT)
  - Coinbase Exchange (BTC/USD)
  - Bitstamp (BTC/USD)
  - Kraken (BTC/USD, BTC/EUR)
  - Bitvavo (BTC/EUR)
- **Backfill**: REST fetch with deterministic timeframe aggregation fallback (e.g., Coinbase 1m → 30m / 4h).
- **Persistence**: Last selected symbol and timeframe saved to disk (JSON). Path is injected from the UI during `{type:"init"}`.

---

## Project Structure & File Count Constraint

- `lib/app_core.dart` – **MANDATORY**: Monolithic, pure-Dart core isolate.
- `lib/main_desktop.dart` – **MANDATORY**: Single-file Flutter desktop UI (Windows, macOS, Linux).
- `lib/main_mobile.dart` – **MANDATORY**: Single-file Flutter mobile UI (Android/iOS).
- `.github/workflows/main.yml` – **MANDATORY**: CI to build unsigned desktop binaries and a debug Android APK.
- `pubspec.yaml` – **MANDATORY**: Pinned dependencies.
- `README.md` – **MANDATORY**: This document.

> **Note:** No other `.dart` files are included. This meets the **“MAX 2 Dart files per platform”** constraint: one shared core + one platform-specific UI entry point.

---

## Build & Run

### Prerequisites

- Flutter SDK (Stable channel)
- Dart SDK (via Flutter)
- For desktop builds: platform-specific toolchains enabled by Flutter.

### Desktop (Windows / macOS / Linux)

Run with the desktop entrypoint:

```bash
flutter pub get
flutter run -t lib/main_desktop.dart -d <device>
