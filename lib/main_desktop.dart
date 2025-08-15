import 'dart:async';
import 'dart:convert';
import 'dart:isolate';

import 'package:candlesticks/candlesticks.dart' as cs;
import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

import 'app_core.dart' as core;

/// Desktop UI with CoreIsolateBridge and chart stack (candles + VWAP + volume).

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final dir = await getApplicationSupportDirectory();
  runApp(DesktopApp(stateDirPath: dir.path));
}

class DesktopApp extends StatelessWidget {
  final String stateDirPath;
  const DesktopApp({super.key, required this.stateDirPath});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Crypto Charts (Desktop)',
      theme: ThemeData(
        colorSchemeSeed: Colors.blue,
        brightness: Brightness.dark,
        useMaterial3: true,
      ),
      home: DesktopHome(stateDirPath: stateDirPath),
      debugShowCheckedModeBanner: false,
    );
  }
}

/// CoreIsolateBridge (desktop)
class CoreIsolateBridge {
  Isolate? _iso;
  SendPort? _coreSend;
  late final ReceivePort _rx;

  final _aggCtl = StreamController<core.AggregatedDataPoint>.broadcast();
  final _candleCtl = StreamController<List<core.Candle>>.broadcast();
  final _statusCtl = StreamController<Map<String, dynamic>>.broadcast();
  final _errorCtl = StreamController<Map<String, dynamic>>.broadcast();

  // Internal buffer for backfill batch
  final List<core.Candle> _buffer = [];
  String? _pendingBackfillReqId;

  Stream<core.AggregatedDataPoint> get aggregatedDataPointStream =>
      _aggCtl.stream;
  Stream<List<core.Candle>> get candleHistoryStream => _candleCtl.stream;
  Stream<Map<String, dynamic>> get connectionStatusStream => _statusCtl.stream;
  Stream<Map<String, dynamic>> get errorStream => _errorCtl.stream;

  Future<void> initialize(String stateDirPath) async {
    _rx = ReceivePort();
    _iso = await Isolate.spawn(core.coreIsolateMain, _rx.sendPort);
    // First message is core's SendPort
    final coreSendPort = await _rx.first as SendPort;
    _coreSend = coreSendPort;
    _rx.listen(_handleInbound);
    // Init
    postCommand({
      'type': 'init',
      'stateDirPath': stateDirPath,
      'debug': false,
      'req_id': 'init-1',
      'ts': DateTime.now().toUtc().toIso8601String()
    });
  }

  void _handleInbound(dynamic msg) {
    try {
      if (msg is String) {
        final m = jsonDecode(msg) as Map<String, dynamic>;
        final type = m['type'] as String?;
        final reqId = m['req_id'] as String?;
        final data = (m['data'] ?? {}) as Map<String, dynamic>;
        switch (type) {
          case 'aggregated':
            _aggCtl.add(core.AggregatedDataPoint.fromMap(data));
            break;
          case 'candle':
            if (_pendingBackfillReqId != null &&
                reqId == _pendingBackfillReqId) {
              _buffer.add(core.Candle.fromMap(data));
            }
            break;
          case 'ack':
            final forCmd = data['for'];
            if (forCmd == 'backfill' &&
                reqId != null &&
                reqId == _pendingBackfillReqId) {
              final snapshot = List<core.Candle>.from(_buffer);
              _buffer.clear();
              _pendingBackfillReqId = null;
              snapshot.sort((a, b) =>
                  a.openTimeUtcS.compareTo(b.openTimeUtcS));
              _candleCtl.add(snapshot);
            }
            break;
          case 'status':
            _statusCtl.add(data);
            break;
          case 'error':
            _errorCtl.add(data);
            break;
        }
      }
    } catch (e) {
      _errorCtl.add({'code': 'DECODE', 'message': e.toString()});
    }
  }

  void postCommand(Map<String, dynamic> command) {
    final payload = jsonEncode({
      ...command,
      'ts': command['ts'] ?? DateTime.now().toUtc().toIso8601String(),
    });
    _coreSend?.send(payload);
    if (command['type'] == 'backfill') {
      _pendingBackfillReqId = command['req_id'] as String?;
      _buffer.clear();
    }
  }

  void dispose() {
    _aggCtl.close();
    _candleCtl.close();
    _statusCtl.close();
    _errorCtl.close();
    _rx.close();
    _iso?.kill(priority: Isolate.immediate);
  }
}

class DesktopHome extends StatefulWidget {
  final String stateDirPath;
  const DesktopHome({super.key, required this.stateDirPath});

  @override
  State<DesktopHome> createState() => _DesktopHomeState();
}

class _DesktopHomeState extends State<DesktopHome> {
  late final CoreIsolateBridge coreBridge;
  final ValueNotifier<String> _symbol =
      ValueNotifier<String>('BTC/USDT');
  final ValueNotifier<String> _timeframe =
      ValueNotifier<String>('1m');

  late final StreamSubscription _errSub;

  // For overlay charts
  final List<core.AggregatedDataPoint> _aggPoints = [];

  @override
  void initState() {
    super.initState();
    coreBridge = CoreIsolateBridge();
    coreBridge.initialize(widget.stateDirPath);
    _errSub = coreBridge.errorStream.listen((e) {
      final ctx = context;
      if (ctx.mounted) {
        ScaffoldMessenger.of(ctx).showSnackBar(
          SnackBar(content: Text('${e['code']}: ${e['message']}')),
        );
      }
    });
    // Initial backfill
    WidgetsBinding.instance.addPostFrameCallback((_) {
      _requestBackfill();
    });
    // Maintain VWAP overlay points
    coreBridge.aggregatedDataPointStream.listen((p) {
      _aggPoints.add(p);
      if (_aggPoints.length > 2000) {
        _aggPoints.removeRange(0, _aggPoints.length - 2000);
      }
      setState(() {});
    });
  }

  @override
  void dispose() {
    _errSub.cancel();
    coreBridge.dispose();
    super.dispose();
  }

  void _requestBackfill() {
    final tf = _timeframe.value;
    final tfSec = core.timeframeSeconds[tf]!;
    final now = DateTime.now().toUtc();
    final start = now.subtract(Duration(seconds: tfSec * 500)); // ~500 bars
    final reqId = 'bf-${now.microsecondsSinceEpoch}';
    coreBridge.postCommand({
      'type': 'backfill',
      'symbol': _symbol.value,
      'timeframe': tf,
      'startIso': start.toIso8601String(),
      'endIso': now.toIso8601String(),
      'req_id': reqId,
    });
  }

  List<String> get _symbols => const ['BTC/USDT', 'BTC/USD', 'BTC/EUR'];
  List<String> get _timeframes =>
      core.supportedTimeframes.toList();

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Crypto Charts (Desktop)'),
        actions: [
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 8),
            child: _SymbolSelector(
              symbols: _symbols,
              valueListenable: _symbol,
              onChanged: (v) {
                _symbol.value = v;
                coreBridge.postCommand(
                    {'type': 'setSymbol', 'symbol': v, 'req_id': 'sym'});
                _requestBackfill();
              },
            ),
          ),
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 8),
            child: _TimeframeSelector(
              timeframes: _timeframes,
              valueListenable: _timeframe,
              onChanged: (v) {
                _timeframe.value = v;
                coreBridge.postCommand(
                    {'type': 'setTimeframe', 'timeframe': v, 'req_id': 'tf'});
                _requestBackfill();
              },
            ),
          ),
        ],
      ),
      body: Column(
        children: [
          Expanded(
            child: Stack(
              children: [
                // Candlestick chart
                StreamBuilder<List<core.Candle>>(
                  stream: coreBridge.candleHistoryStream,
                  builder: (context, snap) {
                    final data = (snap.data ?? []);
                    final csCandles = data
                        .map((c) => cs.Candle(
                              date: DateTime.fromMillisecondsSinceEpoch(
                                  c.openTimeUtcS * 1000,
                                  isUtc: true),
                              high: double.parse(core.fixedToString(c.highInt)),
                              low: double.parse(core.fixedToString(c.lowInt)),
                              open: double.parse(core.fixedToString(c.openInt)),
                              close:
                                  double.parse(core.fixedToString(c.closeInt)),
                              volume: double
                                  .parse(core.fixedToString(c.volumeInt, decimals: 4)),
                            ))
                        .toList();
                    return Padding(
                      padding: const EdgeInsets.only(bottom: 80),
                      child: cs.Candlesticks(
                        candles: csCandles,
                      ),
                    );
                  },
                ),
                // VWAP overlay (LineChart)
                Positioned.fill(
                  child: IgnorePointer(
                    child: StreamBuilder<core.AggregatedDataPoint>(
                      stream: coreBridge.aggregatedDataPointStream,
                      builder: (context, snap) {
                        final spots = <FlSpot>[];
                        final baseT = _aggPoints.isNotEmpty
                            ? _aggPoints.first.timestampUtcS.toDouble()
                            : 0.0;
                        for (final p in _aggPoints) {
                          final x = p.timestampUtcS.toDouble() - baseT;
                          final y =
                              double.parse(core.fixedToString(p.vwapInt));
                          spots.add(FlSpot(x, y));
                        }
                        if (spots.length < 2) {
                          return const SizedBox.shrink();
                        }
                        return Padding(
                          padding: const EdgeInsets.only(bottom: 80),
                          child: LineChart(LineChartData(
                            gridData: const FlGridData(show: false),
                            titlesData: const FlTitlesData(show: false),
                            borderData: FlBorderData(show: false),
                            lineBarsData: [
                              LineChartBarData(
                                spots: spots,
                                isCurved: false,
                                dotData: const FlDotData(show: false),
                                belowBarData:
                                    BarAreaData(show: false),
                              )
                            ],
                          )),
                        );
                      },
                    ),
                  ),
                ),
                // Volume overlay (BarChart) at bottom within the stack
                Positioned(
                  left: 0,
                  right: 0,
                  bottom: 0,
                  height: 80,
                  child: StreamBuilder<core.AggregatedDataPoint>(
                    stream: coreBridge.aggregatedDataPointStream,
                    builder: (context, snap) {
                      final bars = <BarChartGroupData>[];
                      final baseT = _aggPoints.isNotEmpty
                          ? _aggPoints.first.timestampUtcS.toDouble()
                          : 0.0;
                      var i = 0;
                      for (final p in _aggPoints.take(200)) {
                        final vol =
                            double.parse(core.fixedToString(p.volumeInt));
                        bars.add(BarChartGroupData(x: i, barRods: [
                          BarChartRodData(toY: vol)
                        ]));
                        i++;
                      }
                      if (bars.isEmpty) return const SizedBox.shrink();
                      return BarChart(
                        BarChartData(
                          gridData: const FlGridData(show: false),
                          titlesData: const FlTitlesData(show: false),
                          borderData: FlBorderData(show: true),
                          barGroups: bars,
                        ),
                      );
                    },
                  ),
                )
              ],
            ),
          ),
          // Status bar
          StreamBuilder<Map<String, dynamic>>(
            stream: coreBridge.connectionStatusStream,
            builder: (context, snap) {
              final m = snap.data;
              return Container(
                padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
                width: double.infinity,
                color: Colors.black.withOpacity(0.2),
                child: m == null
                    ? const Text('Connecting...')
                    : Row(
                        children: [
                          _StatusDot(connected: (m['connected'] == true)),
                          const SizedBox(width: 8),
                          Text('${m['exchange']}'),
                          const SizedBox(width: 16),
                          Text('Last ingest ns: ${m['lastIngestUtcNs']}'),
                          const SizedBox(width: 16),
                          Text('Latency ~ ${m['latencyMsEstimate']} ms'),
                        ],
                      ),
              );
            },
          ),
        ],
      ),
    );
  }
}

class _SymbolSelector extends StatelessWidget {
  final List<String> symbols;
  final ValueNotifier<String> valueListenable;
  final ValueChanged<String> onChanged;
  const _SymbolSelector({
    required this.symbols,
    required this.valueListenable,
    required this.onChanged,
  });

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<String>(
      valueListenable: valueListenable,
      builder: (context, value, _) {
        return DropdownButton<String>(
          value: value,
          items: symbols
              .map((s) => DropdownMenuItem<String>(
                    value: s,
                    child: Text(s),
                  ))
              .toList(),
          onChanged: (v) {
            if (v != null) onChanged(v);
          },
        );
      },
    );
  }
}

class _TimeframeSelector extends StatelessWidget {
  final List<String> timeframes;
  final ValueNotifier<String> valueListenable;
  final ValueChanged<String> onChanged;
  const _TimeframeSelector({
    required this.timeframes,
    required this.valueListenable,
    required this.onChanged,
  });

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<String>(
      valueListenable: valueListenable,
      builder: (context, value, _) {
        return DropdownButton<String>(
          value: value,
          items: timeframes
              .map((s) => DropdownMenuItem<String>(
                    value: s,
                    child: Text(s),
                  ))
              .toList(),
          onChanged: (v) {
            if (v != null) onChanged(v);
          },
        );
      },
    );
  }
}

class _StatusDot extends StatelessWidget {
  final bool connected;
  const _StatusDot({required this.connected});
  @override
  Widget build(BuildContext context) {
    return Container(
      width: 12,
      height: 12,
      decoration: BoxDecoration(
        color: connected ? Colors.green : Colors.red,
        shape: BoxShape.circle,
      ),
    );
  }
}
