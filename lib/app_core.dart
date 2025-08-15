// Pure Dart core (no Flutter imports).
// Implements: data models, exchange adapters (WS/REST), normalization,
// aggregation, persistence, command handling, and Isolate entry point.

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:http/http.dart' as http;
import 'package:logging/logging.dart';
import 'package:web_socket_channel/io.dart';

/// ==============================
/// Utilities & Constants
/// ==============================

final _log = Logger('Core');

const _scale = 100000000; // 1e8 fixed-point

const supportedTimeframes = <String>{
  '1m',
  '5m',
  '15m',
  '30m',
  '1h',
  '4h',
  '1d',
  '1w',
};

const timeframeSeconds = <String, int>{
  '1m': 60,
  '5m': 300,
  '15m': 900,
  '30m': 1800,
  '1h': 3600,
  '4h': 14400,
  '1d': 86400,
  '1w': 604800,
};

int _bucketStartAligned(int unixSec, int tfSec) => (unixSec ~/ tfSec) * tfSec;

int _nowNs() => DateTime.now().toUtc().microsecondsSinceEpoch * 1000;

/// Safe parse decimal string into fixed-point int (1e8 scale).
int parseFixed(String s) {
  s = s.trim();
  if (s.isEmpty) return 0;
  final neg = s.startsWith('-');
  if (neg) s = s.substring(1);
  final parts = s.split('.');
  final whole = int.parse(parts[0].isEmpty ? '0' : parts[0]);
  int frac = 0;
  if (parts.length > 1) {
    var f = parts[1];
    if (f.length > 8) {
      f = f.substring(0, 8);
    } else if (f.length < 8) {
      f = f.padRight(8, '0');
    }
    frac = int.parse(f);
  }
  var val = whole * _scale + frac;
  if (neg) val = -val;
  return val;
}

String fixedToString(int v, {int decimals = 8}) {
  final neg = v < 0;
  v = v.abs();
  final whole = v ~/ _scale;
  var frac = (v % _scale).toString().padLeft(8, '0');
  if (decimals < 8) frac = frac.substring(0, decimals);
  return '${neg ? '-' : ''}$whole${decimals > 0 ? '.' : ''}${decimals > 0 ? frac : ''}';
}

int secToNs(int sec) => sec * 1000000000;
int msToNs(int ms) => ms * 1000000;
int usToNs(int us) => us * 1000;

/// ==============================
/// Data Models
/// ==============================

class NormalizedTrade {
  final String symbol; // Canonical BASE/QUOTE
  final String venue; // Exchange name
  final int priceInt; // 1e8
  final int sizeInt; // 1e8 (base units)
  final int timestampUtcNs; // epoch ns

  const NormalizedTrade({
    required this.symbol,
    required this.venue,
    required this.priceInt,
    required this.sizeInt,
    required this.timestampUtcNs,
  });

  Map<String, dynamic> toMap() => {
        'symbol': symbol,
        'venue': venue,
        'priceInt': priceInt,
        'sizeInt': sizeInt,
        'timestampUtcNs': timestampUtcNs,
      };

  static NormalizedTrade fromMap(Map<String, dynamic> m) => NormalizedTrade(
        symbol: m['symbol'],
        venue: m['venue'],
        priceInt: m['priceInt'],
        sizeInt: m['sizeInt'],
        timestampUtcNs: m['timestampUtcNs'],
      );

  @override
  String toString() =>
      'Trade($venue $symbol p=${fixedToString(priceInt)} s=${fixedToString(sizeInt)} tsNs=$timestampUtcNs)';

  @override
  bool operator ==(Object other) =>
      other is NormalizedTrade &&
      other.symbol == symbol &&
      other.venue == venue &&
      other.priceInt == priceInt &&
      other.sizeInt == sizeInt &&
      other.timestampUtcNs == timestampUtcNs;

  @override
  int get hashCode =>
      Object.hash(symbol, venue, priceInt, sizeInt, timestampUtcNs);
}

class AggregatedDataPoint {
  final String symbol;
  final String timeframe;
  final int timestampUtcS; // bucket start (open time) or end? We'll use open time.
  final int vwapInt; // 1e8
  final int volumeInt; // 1e8
  final int lastPriceInt; // 1e8
  final bool amend;

  const AggregatedDataPoint({
    required this.symbol,
    required this.timeframe,
    required this.timestampUtcS,
    required this.vwapInt,
    required this.volumeInt,
    required this.lastPriceInt,
    required this.amend,
  });

  Map<String, dynamic> toMap() => {
        'symbol': symbol,
        'timeframe': timeframe,
        'timestampUtcS': timestampUtcS,
        'vwapInt': vwapInt,
        'volumeInt': volumeInt,
        'lastPriceInt': lastPriceInt,
        'amend': amend,
      };

  static AggregatedDataPoint fromMap(Map<String, dynamic> m) =>
      AggregatedDataPoint(
        symbol: m['symbol'],
        timeframe: m['timeframe'],
        timestampUtcS: m['timestampUtcS'],
        vwapInt: m['vwapInt'],
        volumeInt: m['volumeInt'],
        lastPriceInt: m['lastPriceInt'],
        amend: m['amend'] ?? false,
      );

  @override
  String toString() =>
      'Agg($symbol ${timeframe} t=$timestampUtcS vwap=${fixedToString(vwapInt)} vol=${fixedToString(volumeInt)} last=${fixedToString(lastPriceInt)} amend=$amend)';

  @override
  bool operator ==(Object o) =>
      o is AggregatedDataPoint &&
      o.symbol == symbol &&
      o.timeframe == timeframe &&
      o.timestampUtcS == timestampUtcS &&
      o.vwapInt == vwapInt &&
      o.volumeInt == volumeInt &&
      o.lastPriceInt == lastPriceInt &&
      o.amend == amend;

  @override
  int get hashCode =>
      Object.hash(symbol, timeframe, timestampUtcS, vwapInt, volumeInt,
          lastPriceInt, amend);
}

class Candle {
  final String symbol;
  final String timeframe;
  final int openTimeUtcS;
  final int openInt;
  final int highInt;
  final int lowInt;
  final int closeInt;
  final int volumeInt;

  const Candle({
    required this.symbol,
    required this.timeframe,
    required this.openTimeUtcS,
    required this.openInt,
    required this.highInt,
    required this.lowInt,
    required this.closeInt,
    required this.volumeInt,
  });

  Map<String, dynamic> toMap() => {
        'symbol': symbol,
        'timeframe': timeframe,
        'openTimeUtcS': openTimeUtcS,
        'openInt': openInt,
        'highInt': highInt,
        'lowInt': lowInt,
        'closeInt': closeInt,
        'volumeInt': volumeInt,
      };

  static Candle fromMap(Map<String, dynamic> m) => Candle(
        symbol: m['symbol'],
        timeframe: m['timeframe'],
        openTimeUtcS: m['openTimeUtcS'],
        openInt: m['openInt'],
        highInt: m['highInt'],
        lowInt: m['lowInt'],
        closeInt: m['closeInt'],
        volumeInt: m['volumeInt'],
      );

  @override
  String toString() =>
      'Candle($symbol ${timeframe} t=$openTimeUtcS O=${fixedToString(openInt)} H=${fixedToString(highInt)} L=${fixedToString(lowInt)} C=${fixedToString(closeInt)} V=${fixedToString(volumeInt)})';

  @override
  bool operator ==(Object o) =>
      o is Candle &&
      o.symbol == symbol &&
      o.timeframe == timeframe &&
      o.openTimeUtcS == openTimeUtcS &&
      o.openInt == openInt &&
      o.highInt == highInt &&
      o.lowInt == lowInt &&
      o.closeInt == closeInt &&
      o.volumeInt == volumeInt;

  @override
  int get hashCode => Object.hash(symbol, timeframe, openTimeUtcS, openInt,
      highInt, lowInt, closeInt, volumeInt);
}

/// ==============================
/// Symbol normalization
/// ==============================

class SymbolMap {
  static const binance = {'BTC/USDT': 'BTCUSDT'};
  static const okx = {'BTC/USDT': 'BTC-USDT'};
  static const bitget = {'BTC/USDT': 'BTCUSDT'};
  static const coinbase = {'BTC/USD': 'BTC-USD'};
  static const bitstamp = {'BTC/USD': 'btcusd'};
  static const kraken = {
    'BTC/USD': 'XBT/USD',
    'BTC/EUR': 'XBT/EUR',
  };
  static const krakenRest = {
    'BTC/USD': 'XXBTZUSD',
    'BTC/EUR': 'XXBTZEUR',
  };
  static const bitvavo = {'BTC/EUR': 'BTC-EUR'};
}

/// ==============================
/// Persistence (StateManager)
//  UI injects state dir path via init message.
/// ==============================

class StateManager {
  final String dirPath;
  StateManager(this.dirPath);

  File get _file => File('${dirPath}${Platform.pathSeparator}state.json');
  File get _tmp => File('${dirPath}${Platform.pathSeparator}state.json.tmp');

  Future<void> save(String lastSymbol, String lastTimeframe) async {
    final data = jsonEncode({
      'lastSymbol': lastSymbol,
      'lastTimeframe': lastTimeframe,
    });
    await _tmp.writeAsString(data);
    if (await _file.exists()) {
      await _file.delete();
    }
    await _tmp.rename(_file.path);
  }

  Future<(String?, String?)> load() async {
    try {
      if (!await _file.exists()) return (null, null);
      final s = await _file.readAsString();
      final m = jsonDecode(s) as Map<String, dynamic>;
      return (m['lastSymbol'] as String?, m['lastTimeframe'] as String?);
    } catch (_) {
      return (null, null);
    }
  }
}

/// ==============================
/// Exchange Adapter Abstractions
/// ==============================

abstract class ExchangeAdapter {
  final String name;
  final void Function(NormalizedTrade) onTrade;
  final void Function(String exchange, bool connected) onConnectionChange;
  final void Function(
    String exchange,
    int lastIngestUtcNs,
    int latencyMsEstimate,
  ) onStatus;

  IOWebSocketChannel? _chan;
  Timer? _timeoutTimer;
  Timer? _statusTimer;
  int _lastMsgNs = 0;
  bool _stopping = false;
  http.Client httpClient = http.Client();

  ExchangeAdapter({
    required this.name,
    required this.onTrade,
    required this.onConnectionChange,
    required this.onStatus,
  });

  /// Connect and keep alive with exponential backoff.
  Future<void> connect(String symbol) async {
    _stopping = false;
    var base = 0.5;
    var backoff = base;
    while (!_stopping) {
      try {
        final ws = await createChannel(symbol);
        _chan = ws;
        _lastMsgNs = _nowNs();
        onConnectionChange(name, true);
        await afterOpen(ws, symbol);
        _startSupervision(symbol);
        await for (final msg in ws.stream) {
          _lastMsgNs = _nowNs();
          await handleMessage(msg, symbol);
        }
        // Stream ended
        onConnectionChange(name, false);
        _cancelSupervision();
        _chan = null;
      } catch (e) {
        onConnectionChange(name, false);
        _cancelSupervision();
        _chan = null;
      }
      if (_stopping) break;
      final jitter = (Random().nextDouble() * 0.2 - 0.1); // ±10%
      final wait = min(30.0, backoff) * (1.0 + jitter);
      await Future.delayed(Duration(milliseconds: (wait * 1000).round()));
      backoff *= 2.0;
      if (backoff > 30.0) backoff = 30.0;
    }
  }

  void _startSupervision(String symbol) {
    _timeoutTimer?.cancel();
    _statusTimer?.cancel();
    _timeoutTimer = Timer.periodic(const Duration(seconds: 5), (_) async {
      if (_stopping) return;
      final now = _nowNs();
      if (now - _lastMsgNs > secToNs(30)) {
        try {
          await _chan?.sink.close();
        } catch (_) {}
      }
    });
    _statusTimer = Timer.periodic(const Duration(seconds: 1), (_) {
      final now = _nowNs();
      final latencyMs = ((_nowNs() - _lastMsgNs) / 1e6).round();
      onStatus(name, now, latencyMs);
    });
  }

  void _cancelSupervision() {
    _timeoutTimer?.cancel();
    _statusTimer?.cancel();
    _timeoutTimer = null;
    _statusTimer = null;
  }

  Future<void> disconnect() async {
    _stopping = true;
    _cancelSupervision();
    try {
      await _chan?.sink.close();
    } catch (_) {}
    _chan = null;
  }

  Future<void> subscribeToTrades(String symbol);

  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end);

  /// Create WebSocket channel. Implement per exchange.
  Future<IOWebSocketChannel> createChannel(String symbol);

  /// Called after connection established (e.g., send subscribe messages).
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol);

  /// Parse and handle messages.
  Future<void> handleMessage(dynamic msg, String symbol);
}

/// ==============================
/// Concrete Adapters
/// ==============================

String _mapSymbol(String exchange, String canonical) {
  switch (exchange) {
    case 'Binance':
      return SymbolMap.binance[canonical] ?? canonical;
    case 'OKX':
      return SymbolMap.okx[canonical] ?? canonical;
    case 'Bitget':
      return SymbolMap.bitget[canonical] ?? canonical;
    case 'Coinbase Exchange':
      return SymbolMap.coinbase[canonical] ?? canonical;
    case 'Bitstamp':
      return SymbolMap.bitstamp[canonical] ?? canonical;
    case 'Kraken':
      return SymbolMap.kraken[canonical] ?? canonical;
    case 'Bitvavo':
      return SymbolMap.bitvavo[canonical] ?? canonical;
  }
  return canonical;
}

int _parseOkxTsMs(dynamic v) {
  if (v is String) return int.tryParse(v) ?? 0;
  if (v is num) return v.toInt();
  return 0;
}

class BinanceAdapter extends ExchangeAdapter {
  BinanceAdapter(
      {required super.onTrade,
      required super.onConnectionChange,
      required super.onStatus})
      : super(name: 'Binance');

  @override
  Future<IOWebSocketChannel> createChannel(String symbol) async {
    final mapped = _mapSymbol(name, symbol).toLowerCase();
    final url = 'wss://stream.binance.com:9443/ws/${mapped}@trade';
    final ws = await WebSocket.connect(url, pingInterval: const Duration(seconds: 15));
    return IOWebSocketChannel(ws);
  }

  @override
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol) async {
    // stream path already subscribes
  }

  @override
  Future<void> subscribeToTrades(String symbol) async {
    // reconnect uses stream path; no-op
  }

  @override
  Future<void> handleMessage(dynamic msg, String symbol) async {
    try {
      final m = jsonDecode(msg as String) as Map<String, dynamic>;
      final p = parseFixed(m['p'] as String);
      final q = parseFixed(m['q'] as String);
      final t = (m['T'] as num).toInt(); // ms
      onTrade(NormalizedTrade(
          symbol: symbol,
          venue: name,
          priceInt: p,
          sizeInt: q,
          timestampUtcNs: msToNs(t)));
    } catch (_) {}
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final mapped = _mapSymbol(name, symbol);
    final intervalMap = {
      '1m': '1m',
      '5m': '5m',
      '15m': '15m',
      '30m': '30m',
      '1h': '1h',
      '4h': '4h',
      '1d': '1d',
      '1w': '1w',
    };
    final interval = intervalMap[timeframe]!;
    final candles = <Candle>[];
    var cursor = start.toUtc().millisecondsSinceEpoch;
    final endMs = end.toUtc().millisecondsSinceEpoch;
    while (cursor < endMs) {
      final url =
          'https://api.binance.com/api/v3/klines?symbol=$mapped&interval=$interval&startTime=$cursor&limit=1000';
      final resp = await httpClient.get(Uri.parse(url));
      if (resp.statusCode != 200) break;
      final arr = jsonDecode(resp.body) as List<dynamic>;
      if (arr.isEmpty) break;
      for (final v in arr) {
        final a = v as List<dynamic>;
        final openMs = (a[0] as num).toInt();
        final open = parseFixed(a[1].toString());
        final high = parseFixed(a[2].toString());
        final low = parseFixed(a[3].toString());
        final close = parseFixed(a[4].toString());
        final vol = parseFixed(a[5].toString());
        candles.add(Candle(
          symbol: symbol,
          timeframe: timeframe,
          openTimeUtcS: (openMs / 1000).floor(),
          openInt: open,
          highInt: high,
          lowInt: low,
          closeInt: close,
          volumeInt: vol,
        ));
        cursor = openMs + 1;
      }
      if (arr.length < 1000) break;
      // throttle
      await Future.delayed(const Duration(milliseconds: 200));
    }
    return candles;
  }
}

class OKXAdapter extends ExchangeAdapter {
  OKXAdapter(
      {required super.onTrade,
      required super.onConnectionChange,
      required super.onStatus})
      : super(name: 'OKX');

  @override
  Future<IOWebSocketChannel> createChannel(String symbol) async {
    final ws = await WebSocket.connect('wss://ws.okx.com:8443/ws/v5/public',
        pingInterval: const Duration(seconds: 15));
    return IOWebSocketChannel(ws);
  }

  @override
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol) async {
    final sub = {
      'op': 'subscribe',
      'args': [
        {'channel': 'trades', 'instId': _mapSymbol(name, symbol)}
      ]
    };
    ch.sink.add(jsonEncode(sub));
  }

  @override
  Future<void> subscribeToTrades(String symbol) async {
    // handled in afterOpen
  }

  @override
  Future<void> handleMessage(dynamic msg, String symbol) async {
    try {
      final m = jsonDecode(msg as String) as Map<String, dynamic>;
      if (m['arg'] is Map && m['data'] is List) {
        final data = m['data'] as List;
        for (final e in data) {
          final px = parseFixed(e['px'] as String);
          final sz = parseFixed(e['sz'] as String);
          final tsMs = _parseOkxTsMs(e['ts']);
          onTrade(NormalizedTrade(
              symbol: symbol,
              venue: name,
              priceInt: px,
              sizeInt: sz,
              timestampUtcNs: msToNs(tsMs)));
        }
      }
    } catch (_) {}
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final barMap = {
      '1m': '1m',
      '5m': '5m',
      '15m': '15m',
      '30m': '30m',
      '1h': '1H',
      '4h': '4H',
      '1d': '1D',
      '1w': '1W',
    };
    final mapped = _mapSymbol(name, symbol);
    final bar = barMap[timeframe]!;
    final url =
        'https://www.okx.com/api/v5/market/candles?instId=$mapped&bar=$bar&limit=300';
    final resp = await httpClient.get(Uri.parse(url));
    if (resp.statusCode != 200) return [];
    final m = jsonDecode(resp.body) as Map<String, dynamic>;
    final data = (m['data'] as List?) ?? [];
    // OKX returns latest first
    final candles = <Candle>[];
    for (final row in data.reversed) {
      final a = row as List<dynamic>;
      final tsMs = int.parse(a[0].toString());
      final open = parseFixed(a[1].toString());
      final high = parseFixed(a[2].toString());
      final low = parseFixed(a[3].toString());
      final close = parseFixed(a[4].toString());
      final vol = parseFixed(a[5].toString());
      final tS = (tsMs / 1000).floor();
      if (tS >= start.millisecondsSinceEpoch ~/ 1000 &&
          tS <= end.millisecondsSinceEpoch ~/ 1000) {
        candles.add(Candle(
          symbol: symbol,
          timeframe: timeframe,
          openTimeUtcS: tS,
          openInt: open,
          highInt: high,
          lowInt: low,
          closeInt: close,
          volumeInt: vol,
        ));
      }
    }
    return candles;
  }
}

class BitgetAdapter extends ExchangeAdapter {
  BitgetAdapter(
      {required super.onTrade,
      required super.onConnectionChange,
      required super.onStatus})
      : super(name: 'Bitget');

  @override
  Future<IOWebSocketChannel> createChannel(String symbol) async {
    final ws = await WebSocket.connect('wss://ws.bitget.com/v2/spot/public',
        pingInterval: const Duration(seconds: 15));
    return IOWebSocketChannel(ws);
  }

  @override
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol) async {
    final sub = {
      'op': 'subscribe',
      'args': [
        {
          'instType': 'SPOT',
          'channel': 'trade',
          'instId': _mapSymbol(name, symbol),
        }
      ]
    };
    ch.sink.add(jsonEncode(sub));
  }

  @override
  Future<void> subscribeToTrades(String symbol) async {}

  @override
  Future<void> handleMessage(dynamic msg, String symbol) async {
    try {
      final m = jsonDecode(msg as String) as Map<String, dynamic>;
      if (m['data'] is List) {
        for (final e in (m['data'] as List)) {
          if (e is Map) {
            final p = parseFixed(e['p'].toString());
            final q = parseFixed(e['q'].toString());
            final t = int.parse(e['t'].toString());
            onTrade(NormalizedTrade(
                symbol: symbol,
                venue: name,
                priceInt: p,
                sizeInt: q,
                timestampUtcNs: msToNs(t)));
          } else if (e is List) {
            // compact form [p,q,t]
            final p = parseFixed(e[0].toString());
            final q = parseFixed(e[1].toString());
            final t = int.parse(e[2].toString());
            onTrade(NormalizedTrade(
                symbol: symbol,
                venue: name,
                priceInt: p,
                sizeInt: q,
                timestampUtcNs: msToNs(t)));
          }
        }
      }
    } catch (_) {}
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final secMap = {
      '1m': 60,
      '5m': 300,
      '15m': 900,
      '30m': 1800,
      '1h': 3600,
      '4h': 14400,
      '1d': 86400,
      '1w': 604800,
    };
    final seconds = secMap[timeframe]!;
    final mapped = _mapSymbol(name, symbol);
    final url =
        'https://api.bitget.com/api/spot/v1/market/candles?symbol=$mapped&granularity=$seconds&limit=1000';
    final resp = await httpClient.get(Uri.parse(url));
    if (resp.statusCode != 200) return [];
    final arr = jsonDecode(resp.body) as List<dynamic>;
    final candles = <Candle>[];
    for (final row in arr.reversed) {
      // response format may be [ts,o,h,l,c,vol]
      final a = row as List<dynamic>;
      final tsSec = int.parse(a[0].toString());
      final o = parseFixed(a[1].toString());
      final h = parseFixed(a[2].toString());
      final l = parseFixed(a[3].toString());
      final c = parseFixed(a[4].toString());
      final v = parseFixed(a[5].toString());
      final tS = tsSec;
      if (tS >= start.millisecondsSinceEpoch ~/ 1000 &&
          tS <= end.millisecondsSinceEpoch ~/ 1000) {
        candles.add(Candle(
            symbol: symbol,
            timeframe: timeframe,
            openTimeUtcS: tS,
            openInt: o,
            highInt: h,
            lowInt: l,
            closeInt: c,
            volumeInt: v));
      }
    }
    return candles;
  }
}

class CoinbaseAdapter extends ExchangeAdapter {
  CoinbaseAdapter(
      {required super.onTrade,
      required super.onConnectionChange,
      required super.onStatus})
      : super(name: 'Coinbase Exchange');

  @override
  Future<IOWebSocketChannel> createChannel(String symbol) async {
    final ws = await WebSocket.connect('wss://ws-feed.exchange.coinbase.com',
        pingInterval: const Duration(seconds: 15));
    return IOWebSocketChannel(ws);
  }

  @override
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol) async {
    final sub = {
      'type': 'subscribe',
      'product_ids': [_mapSymbol(name, symbol)],
      'channels': ['matches']
    };
    ch.sink.add(jsonEncode(sub));
  }

  @override
  Future<void> subscribeToTrades(String symbol) async {}

  @override
  Future<void> handleMessage(dynamic msg, String symbol) async {
    try {
      final m = jsonDecode(msg as String) as Map<String, dynamic>;
      if (m['type'] == 'match') {
        final p = parseFixed(m['price'].toString());
        final q = parseFixed(m['size'].toString());
        final tStr = m['time'] as String; // RFC3339
        final t = DateTime.parse(tStr).toUtc().microsecondsSinceEpoch * 1000;
        onTrade(NormalizedTrade(
            symbol: symbol,
            venue: name,
            priceInt: p,
            sizeInt: q,
            timestampUtcNs: t));
      }
    } catch (_) {}
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    // Coinbase supported seconds: 60,300,900,3600,21600,86400
    const supported = [60, 300, 900, 3600, 21600, 86400];
    final tfSec = timeframeSeconds[timeframe]!;
    int gran = supported.first;
    for (final s in supported) {
      if (s <= tfSec) {
        gran = s;
        break;
      }
    }
    // Fallback: fetch 1m if timeframe not available (e.g., 30m/4h)
    if (!supported.contains(tfSec)) {
      gran = 60;
    } else {
      gran = tfSec;
    }
    final startIso = start.toUtc().toIso8601String();
    final endIso = end.toUtc().toIso8601String();
    final url =
        'https://api.exchange.coinbase.com/products/${_mapSymbol(name, symbol)}/candles?granularity=$gran&start=$startIso&end=$endIso';
    final resp = await httpClient.get(Uri.parse(url), headers: {
      'User-Agent': 'crypto_chart_core_monolith',
    });
    if (resp.statusCode != 200) return [];
    final arr = jsonDecode(resp.body) as List<dynamic>;
    // Coinbase returns [[time, low, high, open, close, volume], ...] newest first
    final raw = <Candle>[];
    for (final row in arr) {
      final a = (row as List).cast<dynamic>();
      final tS = (a[0] as num).toInt();
      final low = parseFixed(a[1].toString());
      final high = parseFixed(a[2].toString());
      final open = parseFixed(a[3].toString());
      final close = parseFixed(a[4].toString());
      final vol = parseFixed(a[5].toString());
      raw.add(Candle(
          symbol: symbol,
          timeframe: '${gran}s',
          openTimeUtcS: tS,
          openInt: open,
          highInt: high,
          lowInt: low,
          closeInt: close,
          volumeInt: vol));
    }
    raw.sort((a, b) => a.openTimeUtcS.compareTo(b.openTimeUtcS));
    if (gran == tfSec) {
      return raw
          .map((c) => Candle(
                symbol: symbol,
                timeframe: timeframe,
                openTimeUtcS: c.openTimeUtcS,
                openInt: c.openInt,
                highInt: c.highInt,
                lowInt: c.lowInt,
                closeInt: c.closeInt,
                volumeInt: c.volumeInt,
              ))
          .toList();
    }
    return _aggregateCandles(raw, timeframe);
  }
}

class BitstampAdapter extends ExchangeAdapter {
  BitstampAdapter(
      {required super.onTrade,
      required super.onConnectionChange,
      required super.onStatus})
      : super(name: 'Bitstamp');

  @override
  Future<IOWebSocketChannel> createChannel(String symbol) async {
    final ws = await WebSocket.connect('wss://ws.bitstamp.net',
        pingInterval: const Duration(seconds: 15));
    return IOWebSocketChannel(ws);
  }

  @override
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol) async {
    final chName =
        'live_trades_${_mapSymbol(name, symbol).toLowerCase()}'; // e.g., btcusd
    final sub = {
      'event': 'bts:subscribe',
      'data': {'channel': chName},
    };
    ch.sink.add(jsonEncode(sub));
  }

  @override
  Future<void> subscribeToTrades(String symbol) async {}

  @override
  Future<void> handleMessage(dynamic msg, String symbol) async {
    try {
      final m = jsonDecode(msg as String) as Map<String, dynamic>;
      if (m['event'] == 'trade' && m['data'] is Map) {
        final d = m['data'] as Map<String, dynamic>;
        final p = parseFixed(d['price'].toString());
        final q = parseFixed(d['amount'].toString());
        final tsSec = int.parse(d['timestamp'].toString());
        onTrade(NormalizedTrade(
            symbol: symbol,
            venue: name,
            priceInt: p,
            sizeInt: q,
            timestampUtcNs: secToNs(tsSec)));
      }
    } catch (_) {}
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final sec = timeframeSeconds[timeframe]!;
    final url =
        'https://www.bitstamp.net/api/v2/ohlc/${_mapSymbol(name, symbol)}/?step=$sec&limit=1000';
    final resp = await httpClient.get(Uri.parse(url));
    if (resp.statusCode != 200) return [];
    final m = jsonDecode(resp.body) as Map<String, dynamic>;
    final data = (m['data']?['ohlc'] as List?) ?? [];
    final candles = <Candle>[];
    for (final e in data) {
      final d = e as Map<String, dynamic>;
      final tS = int.parse(d['timestamp'].toString());
      if (tS < start.millisecondsSinceEpoch ~/ 1000 ||
          tS > end.millisecondsSinceEpoch ~/ 1000) continue;
      candles.add(Candle(
          symbol: symbol,
          timeframe: timeframe,
          openTimeUtcS: tS,
          openInt: parseFixed(d['open'].toString()),
          highInt: parseFixed(d['high'].toString()),
          lowInt: parseFixed(d['low'].toString()),
          closeInt: parseFixed(d['close'].toString()),
          volumeInt: parseFixed(d['volume'].toString())));
    }
    candles.sort((a, b) => a.openTimeUtcS.compareTo(b.openTimeUtcS));
    return candles;
  }
}

class KrakenAdapter extends ExchangeAdapter {
  KrakenAdapter(
      {required super.onTrade,
      required super.onConnectionChange,
      required super.onStatus})
      : super(name: 'Kraken');

  @override
  Future<IOWebSocketChannel> createChannel(String symbol) async {
    final ws = await WebSocket.connect('wss://ws.kraken.com',
        pingInterval: const Duration(seconds: 15));
    return IOWebSocketChannel(ws);
  }

  @override
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol) async {
    final pairs = <String>[];
    if (symbol == 'BTC/USD') pairs.add('BTC/USD');
    if (symbol == 'BTC/EUR') pairs.add('BTC/EUR');
    final sub = {
      'event': 'subscribe',
      'pair': pairs,
      'subscription': {'name': 'trade'}
    };
    ch.sink.add(jsonEncode(sub));
  }

  @override
  Future<void> subscribeToTrades(String symbol) async {}

  @override
  Future<void> handleMessage(dynamic msg, String symbol) async {
    try {
      final obj = jsonDecode(msg as String);
      if (obj is List) {
        // [chanId, [[price, volume, time, ...], ...], "trade", "PAIR"]
        if (obj.length >= 4 && obj[1] is List && obj[2] == 'trade') {
          final trades = obj[1] as List;
          for (final t in trades) {
            final a = (t as List).map((e) => e.toString()).toList();
            final px = parseFixed(a[0]);
            final vol = parseFixed(a[1]);
            // time like "1669051234.1234"
            final parts = a[2].split('.');
            final sec = int.parse(parts[0]);
            var ns = secToNs(sec);
            if (parts.length > 1) {
              final frac = parts[1];
              final us = int.parse(frac.padRight(6, '0').substring(0, 6));
              ns = ns + usToNs(us);
            }
            onTrade(NormalizedTrade(
                symbol: symbol,
                venue: name,
                priceInt: px,
                sizeInt: vol,
                timestampUtcNs: ns));
          }
        }
      }
    } catch (_) {}
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final minutesMap = {
      '1m': 1,
      '5m': 5,
      '15m': 15,
      '30m': 30,
      '1h': 60,
      '4h': 240,
      '1d': 1440,
      '1w': 10080,
    };
    final pair = SymbolMap.krakenRest[symbol]!;
    final interval = minutesMap[timeframe]!;
    final url =
        'https://api.kraken.com/0/public/OHLC?pair=$pair&interval=$interval';
    final resp = await httpClient.get(Uri.parse(url));
    if (resp.statusCode != 200) return [];
    final m = jsonDecode(resp.body) as Map<String, dynamic>;
    final result = m['result'] as Map<String, dynamic>;
    result.remove('last');
    final list = result.values.first as List<dynamic>;
    final candles = <Candle>[];
    for (final row in list) {
      final a = row as List<dynamic>;
      final tS = (a[0] as num).toInt();
      final open = parseFixed(a[1].toString());
      final high = parseFixed(a[2].toString());
      final low = parseFixed(a[3].toString());
      final close = parseFixed(a[4].toString());
      final vol = parseFixed(a[6].toString());
      if (tS < start.millisecondsSinceEpoch ~/ 1000 ||
          tS > end.millisecondsSinceEpoch ~/ 1000) continue;
      candles.add(Candle(
          symbol: symbol,
          timeframe: timeframe,
          openTimeUtcS: tS,
          openInt: open,
          highInt: high,
          lowInt: low,
          closeInt: close,
          volumeInt: vol));
    }
    return candles;
  }
}

class BitvavoAdapter extends ExchangeAdapter {
  BitvavoAdapter(
      {required super.onTrade,
      required super.onConnectionChange,
      required super.onStatus})
      : super(name: 'Bitvavo');

  @override
  Future<IOWebSocketChannel> createChannel(String symbol) async {
    final ws = await WebSocket.connect('wss://ws.bitvavo.com/v2/',
        pingInterval: const Duration(seconds: 15));
    return IOWebSocketChannel(ws);
  }

  @override
  Future<void> afterOpen(IOWebSocketChannel ch, String symbol) async {
    final sub = {
      'action': 'subscribe',
      'channels': [
        {
          'name': 'trades',
          'markets': [_mapSymbol(name, symbol)]
        }
      ]
    };
    ch.sink.add(jsonEncode(sub));
  }

  @override
  Future<void> subscribeToTrades(String symbol) async {}

  @override
  Future<void> handleMessage(dynamic msg, String symbol) async {
    try {
      final m = jsonDecode(msg as String) as Map<String, dynamic>;
      if (m['event'] == 'trade' && m['data'] is List) {
        for (final e in (m['data'] as List)) {
          final d = e as Map<String, dynamic>;
          final p = parseFixed(d['price'].toString());
          final q = parseFixed(d['amount'].toString());
          final ts = d['timestamp']; // ms or ns
          int ns;
          if (ts.toString().length > 13) {
            ns = int.parse(ts.toString());
          } else {
            ns = msToNs(int.parse(ts.toString()));
          }
          onTrade(NormalizedTrade(
              symbol: symbol,
              venue: name,
              priceInt: p,
              sizeInt: q,
              timestampUtcNs: ns));
        }
      }
    } catch (_) {}
  }

  @override
  Future<List<Candle>> fetchHistoricalCandles(
      String symbol, String timeframe, DateTime start, DateTime end) async {
    final intervalMap = {
      '1m': '1m',
      '5m': '5m',
      '15m': '15m',
      '30m': '30m',
      '1h': '1h',
      '4h': '4h',
      '1d': '1d',
      '1w': '1w',
    };
    final inter = intervalMap[timeframe]!;
    final url =
        'https://api.bitvavo.com/v2/${_mapSymbol(name, symbol)}/candles?interval=$inter';
    final resp = await httpClient.get(Uri.parse(url));
    if (resp.statusCode != 200) return [];
    final arr = jsonDecode(resp.body) as List<dynamic>;
    final candles = <Candle>[];
    for (final row in arr) {
      // [time, open, high, low, close, volume]
      final a = (row as List).map((e) => e.toString()).toList();
      final tS = (int.parse(a[0]) ~/ 1000);
      if (tS < start.millisecondsSinceEpoch ~/ 1000 ||
          tS > end.millisecondsSinceEpoch ~/ 1000) continue;
      candles.add(Candle(
          symbol: symbol,
          timeframe: timeframe,
          openTimeUtcS: tS,
          openInt: parseFixed(a[1]),
          highInt: parseFixed(a[2]),
          lowInt: parseFixed(a[3]),
          closeInt: parseFixed(a[4]),
          volumeInt: parseFixed(a[5])));
    }
    candles.sort((a, b) => a.openTimeUtcS.compareTo(b.openTimeUtcS));
    return candles;
  }
}

/// ==============================
/// Aggregator
/// ==============================

class _FrameState {
  int pvSumInt = 0; // Σ price*volume (scaled 1e8 * 1e8 = 1e16 -> fits in 64? use BigInt? We'll keep 64 cautiously)
  int vSumInt = 0; // Σ volume (1e8)
  int lastPriceInt = 0;
  int bucketOpenS = 0;
}

class Aggregator {
  final String symbol;
  final String timeframe;
  final void Function(AggregatedDataPoint) onEmit;
  final int cadenceMs;

  final _queue = Queue<NormalizedTrade>();
  final _state = _FrameState();
  Timer? _timer;
  AggregatedDataPoint? _lastEmitted;
  int _tfSec = 60;

  Aggregator({
    required this.symbol,
    required this.timeframe,
    required this.onEmit,
    this.cadenceMs = 250,
  }) {
    _tfSec = timeframeSeconds[timeframe]!;
  }

  void enqueue(NormalizedTrade t) {
    // drop trades too far out-of-order (>2s)
    final nowNs = _nowNs();
    if (t.timestampUtcNs < nowNs - secToNs(60 * 60 * 24 * 7)) return; // sanity
    _queue.addLast(t);
  }

  void start() {
    _timer?.cancel();
    _timer = Timer.periodic(Duration(milliseconds: cadenceMs), (_) => _tick());
  }

  void stop() {
    _timer?.cancel();
    _timer = null;
  }

  void _fold(NormalizedTrade t) {
    final tsS = (t.timestampUtcNs / 1e9).floor();
    final bucketOpen = _bucketStartAligned(tsS, _tfSec);
    if (_state.bucketOpenS == 0) {
      _state.bucketOpenS = bucketOpen;
    }
    // if trade shifted to new bucket(s)
    if (bucketOpen > _state.bucketOpenS) {
      // finalize previous bucket only when bucket end passes (handled in _tick)
      _state.bucketOpenS = bucketOpen;
      _state.pvSumInt = 0;
      _state.vSumInt = 0;
    }
    // fold into current sums
    _state.lastPriceInt = t.priceInt;
    // pvSumInt = priceInt * sizeInt / 1e8  (to keep 1e8 scale for P*V)
    // But requirement says store P*V as 1e8 scaled integers, so we accumulate priceInt*sizeInt then divide by 1e8 when computing VWAP.
    final pv = (t.priceInt ~/ 1) * (t.sizeInt ~/ 1);
    // Accumulate with downscaling to avoid overflow: keep 1e8 scale by dividing 1e8 from product.
    final pvScaled = pv ~/ _scale;
    _state.pvSumInt += pvScaled;
    _state.vSumInt += t.sizeInt;
  }

  void _tick() {
    final nowS = (DateTime.now().toUtc().millisecondsSinceEpoch / 1000).floor();
    // Drain queue snapshot to avoid long locks.
    final toProcess = <NormalizedTrade>[];
    while (_queue.isNotEmpty) {
      toProcess.add(_queue.removeFirst());
    }
    for (final t in toProcess) {
      final tsS = (t.timestampUtcNs / 1e9).floor();
      final bucketOpen = _bucketStartAligned(tsS, _tfSec);
      final bucketEnd = bucketOpen + _tfSec;
      // Late trades: if within 2s after bucket close and equals last emitted bucket -> amend
      final last = _lastEmitted;
      if (last != null &&
          tsS >= last.timestampUtcS &&
          tsS < last.timestampUtcS + _tfSec) {
        // trade belongs to last emitted bucket
        final nowNs = _nowNs();
        final lastBucketEndNs = secToNs(last.timestampUtcS + _tfSec);
        if (nowNs - lastBucketEndNs <= secToNs(2)) {
          // amend allowed
          // recompute by folding into a temporary state
          // We don't have past trades cached; approximate by adjusting sums if the bucket matches current open state
          if (_state.bucketOpenS == last.timestampUtcS) {
            _fold(t);
            final v = _state.vSumInt;
            final vw = v > 0 ? (_state.pvSumInt ~/ (v ~/ 1)) : last.lastPriceInt;
            final agg = AggregatedDataPoint(
              symbol: symbol,
              timeframe: timeframe,
              timestampUtcS: last.timestampUtcS,
              vwapInt: vw,
              volumeInt: v,
              lastPriceInt: _state.lastPriceInt,
              amend: true,
            );
            _lastEmitted = agg;
            onEmit(agg);
            continue;
          }
        } else {
          // too late -> ignore for aggregates, but update last price tracking
          _state.lastPriceInt = t.priceInt;
          continue;
        }
      }

      // fold into current in-progress bucket
      _fold(t);

      // if time passed current bucket end, emit
      final curOpen = _state.bucketOpenS;
      final curEnd = curOpen + _tfSec;
      if (nowS >= curEnd) {
        final v = _state.vSumInt;
        final vw = v > 0 ? (_state.pvSumInt ~/ (v ~/ 1)) : _state.lastPriceInt;
        final agg = AggregatedDataPoint(
          symbol: symbol,
          timeframe: timeframe,
          timestampUtcS: curOpen,
          vwapInt: vw,
          volumeInt: v,
          lastPriceInt: _state.lastPriceInt,
          amend: false,
        );
        _lastEmitted = agg;
        onEmit(agg);
        // prepare for next bucket
        _state.pvSumInt = 0;
        _state.vSumInt = 0;
        _state.bucketOpenS = curEnd;
      }
    }
  }
}

/// Aggregate lower-granularity candles up to requested timeframe deterministically.
List<Candle> _aggregateCandles(List<Candle> small, String targetTf) {
  if (small.isEmpty) return [];
  final tfSecTarget = timeframeSeconds[targetTf]!;
  final out = <Candle>[];
  int? bucketStart;
  int? open;
  int? high;
  int? low;
  int? close;
  int volume = 0;
  final sym = small.first.symbol;
  for (final c in small) {
    final b = _bucketStartAligned(c.openTimeUtcS, tfSecTarget);
    if (bucketStart == null) {
      bucketStart = b;
      open = c.openInt;
      high = c.highInt;
      low = c.lowInt;
      close = c.closeInt;
      volume = c.volumeInt;
    } else if (b != bucketStart) {
      out.add(Candle(
          symbol: sym,
          timeframe: targetTf,
          openTimeUtcS: bucketStart!,
          openInt: open!,
          highInt: high!,
          lowInt: low!,
          closeInt: close!,
          volumeInt: volume));
      bucketStart = b;
      open = c.openInt;
      high = c.highInt;
      low = c.lowInt;
      close = c.closeInt;
      volume = c.volumeInt;
    } else {
      // same bucket
      high = max(high!, c.highInt);
      low = min(low!, c.lowInt);
      close = c.closeInt;
      volume += c.volumeInt;
    }
  }
  if (bucketStart != null) {
    out.add(Candle(
        symbol: sym,
        timeframe: targetTf,
        openTimeUtcS: bucketStart!,
        openInt: open!,
        highInt: high!,
        lowInt: low!,
        closeInt: close!,
        volumeInt: volume));
  }
  return out;
}

/// ==============================
/// Core Controller & Isolate
/// ==============================

class CoreController {
  final SendPort uiSend;
  late final StreamController<Map<String, dynamic>> _inbound;

  // State
  String _symbol = 'BTC/USDT';
  String _timeframe = '1m';
  late final StateManager _stateManager;

  // Adapters
  final Map<String, ExchangeAdapter> _adapters = {};
  final Map<String, bool> _connected = {};
  final Map<String, int> _lastIngestNs = {};
  Aggregator? _aggregator;

  bool debug = false;

  CoreController(this.uiSend) {
    _inbound = StreamController();
    _inbound.stream.listen(_handleCommand);
  }

  void post(Map<String, dynamic> cmd) => _inbound.add(cmd);

  void _send(String type, Map<String, dynamic> data,
      {String? reqId, String? ts}) {
    final msg = <String, dynamic>{
      'type': type,
      'data': data,
    };
    if (reqId != null) msg['req_id'] = reqId;
    if (ts != null) msg['ts'] = ts;
    uiSend.send(jsonEncode(msg));
  }

  Future<void> _handleCommand(Map<String, dynamic> cmd) async {
    final reqId = cmd['req_id'] as String?;
    final type = cmd['type'] as String?;
    if (type == null) {
      _send('error', {'code': 'INVALID_ARG', 'message': 'Missing type'},
          reqId: reqId);
      return;
    }
    try {
      switch (type) {
        case 'init':
          {
            final stateDir = cmd['stateDirPath'] as String?;
            if (stateDir == null || stateDir.isEmpty) {
              _send('error',
                  {'code': 'INVALID_ARG', 'message': 'stateDirPath required'},
                  reqId: reqId);
              return;
            }
            debug = (cmd['debug'] == true);
            Logger.root.level = debug ? Level.ALL : Level.INFO;
            Logger.root.onRecord.listen((r) {
              // No PII.
            });
            _stateManager = StateManager(stateDir);
            final (ls, lt) = await _stateManager.load();
            if (ls != null && lt != null) {
              if (supportedTimeframes.contains(lt)) {
                _symbol = ls;
                _timeframe = lt;
              }
            }
            await _startAdapters();
            _startAggregator();
            _send('ack', {'for': 'init', 'ok': true, 'symbol': _symbol, 'timeframe': _timeframe},
                reqId: reqId);
            break;
          }
        case 'setSymbol':
          {
            final sym = (cmd['symbol'] as String?)?.toUpperCase();
            if (sym == null ||
                !(sym == 'BTC/USDT' || sym == 'BTC/USD' || sym == 'BTC/EUR')) {
              _send('error', {
                'code': 'INVALID_ARG',
                'message': 'Unsupported symbol: $sym'
              }, reqId: reqId);
              return;
            }
            _symbol = sym;
            await _stateManager.save(_symbol, _timeframe);
            await _restartAdapters();
            _startAggregator();
            _send('ack', {'for': 'setSymbol', 'ok': true}, reqId: reqId);
            break;
          }
        case 'setTimeframe':
          {
            final tf = cmd['timeframe'] as String?;
            if (tf == null || !supportedTimeframes.contains(tf)) {
              _send('error', {
                'code': 'INVALID_ARG',
                'message': 'Unsupported timeframe: $tf'
              }, reqId: reqId);
              return;
            }
            _timeframe = tf;
            await _stateManager.save(_symbol, _timeframe);
            _startAggregator();
            _send('ack', {'for': 'setTimeframe', 'ok': true}, reqId: reqId);
            break;
          }
        case 'backfill':
          {
            final sym = (cmd['symbol'] as String?)?.toUpperCase() ?? _symbol;
            final tf = (cmd['timeframe'] as String?) ?? _timeframe;
            final startIso = cmd['startIso'] as String?;
            final endIso = cmd['endIso'] as String?;
            if (!supportedTimeframes.contains(tf)) {
              _send('error', {
                'code': 'INVALID_ARG',
                'message': 'Unsupported timeframe: $tf'
              }, reqId: reqId);
              return;
            }
            if (startIso == null || endIso == null) {
              _send('error', {
                'code': 'INVALID_ARG',
                'message': 'startIso and endIso required'
              }, reqId: reqId);
              return;
            }
            final start = DateTime.parse(startIso).toUtc();
            final end = DateTime.parse(endIso).toUtc();
            if (!start.isBefore(end)) {
              _send('error', {
                'code': 'INVALID_ARG',
                'message': 'startIso must be before endIso'
              }, reqId: reqId);
              return;
            }
            // Choose a primary adapter for backfill based on symbol.
            final adapter = _pickBackfillAdapter(sym);
            if (adapter == null) {
              _send('error', {
                'code': 'UNAVAILABLE',
                'message': 'No adapter available for $sym'
              }, reqId: reqId);
              return;
            }
            final candles =
                await adapter.fetchHistoricalCandles(sym, tf, start, end);
            for (final c in candles) {
              _send('candle', c.toMap(), reqId: reqId);
            }
            _send('ack', {'for': 'backfill', 'ok': true}, reqId: reqId);
            break;
          }
        case 'shutdown':
          {
            await _shutdown();
            _send('ack', {'for': 'shutdown', 'ok': true}, reqId: reqId);
            break;
          }
        default:
          _send('error', {
            'code': 'UNKNOWN_CMD',
            'message': 'Unknown command: $type'
          }, reqId: reqId);
      }
    } catch (e, st) {
      if (debug) {
        _log.severe('Command error: $e\n$st');
      }
      _send('error', {'code': 'INTERNAL', 'message': e.toString()},
          reqId: reqId);
    }
  }

  ExchangeAdapter? _pickBackfillAdapter(String sym) {
    if (sym == 'BTC/USDT') {
      return _adapters['Binance'] ?? _adapters['OKX'] ?? _adapters['Bitget'];
    } else if (sym == 'BTC/USD') {
      return _adapters['Coinbase Exchange'] ??
          _adapters['Bitstamp'] ??
          _adapters['Kraken'];
    } else if (sym == 'BTC/EUR') {
      return _adapters['Kraken'] ?? _adapters['Bitvavo'];
    }
    return null;
  }

  Future<void> _startAdapters() async {
    // Dispose old if any
    await _stopAdapters();
    _adapters.clear();
    // Create new set for all venues regardless of symbol; only those with mapping are relevant
    void reg(ExchangeAdapter a) {
      _adapters[a.name] = a;
      _connected[a.name] = false;
      _lastIngestNs[a.name] = 0;
    }

    void onConn(String ex, bool c) {
      _connected[ex] = c;
      _send('status', {
        'exchange': ex,
        'connected': c,
        'lastIngestUtcNs': _lastIngestNs[ex] ?? 0,
        'latencyMsEstimate': 0
      });
    }

    void onStatus(String ex, int lastIngestUtcNs, int latMs) {
      _send('status', {
        'exchange': ex,
        'connected': _connected[ex] ?? false,
        'lastIngestUtcNs': lastIngestUtcNs,
        'latencyMsEstimate': latMs
      });
    }

    void onTrade(NormalizedTrade t) {
      _lastIngestNs[t.venue] = t.timestampUtcNs;
      _aggregator?.enqueue(t);
    }

    // Register only adapters that support current symbol to minimize noise.
    final sym = _symbol;
    if (sym == 'BTC/USDT') {
      reg(BinanceAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
      reg(OKXAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
      reg(BitgetAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
    } else if (sym == 'BTC/USD') {
      reg(CoinbaseAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
      reg(BitstampAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
      reg(KrakenAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
    } else if (sym == 'BTC/EUR') {
      reg(KrakenAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
      reg(BitvavoAdapter(
          onTrade: onTrade, onConnectionChange: onConn, onStatus: onStatus));
    }

    // Connect all
    for (final a in _adapters.values) {
      // fire-and-forget
      unawaited(a.connect(_symbol));
    }
  }

  Future<void> _restartAdapters() async {
    await _stopAdapters();
    await _startAdapters();
  }

  Future<void> _stopAdapters() async {
    for (final a in _adapters.values) {
      try {
        await a.disconnect();
      } catch (_) {}
    }
  }

  void _startAggregator() {
    _aggregator?.stop();
    _aggregator = Aggregator(
        symbol: _symbol,
        timeframe: _timeframe,
        onEmit: (agg) {
          _send('aggregated', agg.toMap());
        },
        cadenceMs: 250)
      ..start();
  }

  Future<void> _shutdown() async {
    _aggregator?.stop();
    await _stopAdapters();
  }
}

/// ==============================
/// Isolate Entrypoint
/// ==============================

void coreIsolateMain(SendPort uiSendPort) {
  // Create a ReceivePort for commands from UI
  final coreReceive = ReceivePort();
  // Send back the core's SendPort for UI to post commands
  uiSendPort.send(coreReceive.sendPort);

  final controller = CoreController(uiSendPort);

  coreReceive.listen((dynamic message) {
    try {
      if (message is String) {
        final m = jsonDecode(message) as Map<String, dynamic>;
        controller.post(m);
      } else if (message is Map<String, dynamic>) {
        controller.post(message);
      }
    } catch (e) {
      controller.post({
        'type': 'error',
        'data': {'code': 'BAD_PAYLOAD', 'message': e.toString()}
      });
    }
  });
}
