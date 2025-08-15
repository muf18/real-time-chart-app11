import 'dart:async';
import 'dart:convert';
import 'dart:isolate';

import 'package:candlesticks/candlesticks.dart' as cs;
import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

import 'app_core.dart' as core;

/// Mobile UI with CoreIsolateBridge and drawer selectors.

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final dir = await getApplicationDocumentsDirectory();
  runApp(MobileApp(stateDirPath: dir.path));
}

class MobileApp extends StatelessWidget {
  final String stateDirPath;
  const MobileApp({super.key, required this.stateDirPath});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Crypto Charts (Mobile)',
      theme: ThemeData(
        colorSchemeSeed: Colors.blue,
        brightness: Brightness.dark,
        useMaterial3: true,
      ),
      home: MobileHome(stateDirPath: stateDirPath),
      debugShowCheckedModeBanner: false,
    );
  }
}

/// CoreIsolateBridge (mobile) – same interface as desktop
class CoreIsolateBridge {
  Isolate? _iso;
  SendPort? _coreSend;
  late final ReceivePort _rx;

  final _aggCtl = StreamController<core.AggregatedDataPoint>.broadcast();
  final _candleCtl = StreamController<List<core.Candle>>.broadcast();
  final _statusCtl = StreamController<Map<String, dynamic>>.broadcast();
  final _errorCtl = StreamController<Map<String, dynamic>>.broadcast();

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
    final coreSendPort = await _rx.first as SendPort;
    _coreSend = coreSendPort;
    _rx.listen(_handleInbound);
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
            if (data['for'] == 'backfill' &&
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

class MobileHome extends StatefulWidget {
  final String stateDirPath;
  const MobileHome({super.key, required this.stateDirPath});

  @override
  State<MobileHome> createState() => _MobileHomeState();
}

class _MobileHomeState extends State<MobileHome> {
  late final CoreIsolateBridge coreBridge;
  final ValueNotifier<String> _symbol =
      ValueNotifier<String>('BTC/USDT');
  final ValueNotifier<String> _timeframe =
      ValueNotifier<String>('1m');

  late final StreamSubscription _errSub;

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
    WidgetsBinding.instance.addPostFrameCallback((_) {
      _requestBackfill();
    });
    coreBridge.aggregatedDataPointStream.listen((p) {
      _aggPoints.add(p);
      if (_aggPoints.length > 1500) {
        _aggPoints.removeRange(0, _aggPoints.length - 1500);
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
    final start = now.subtract(Duration(seconds: tfSec * 400));
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
        title: const Text('Crypto Charts (Mobile)'),
      ),
      drawer: Drawer(
        child: SafeArea(
          child: Column(
            children: [
              const ListTile(title: Text('Select Pair')),
              Expanded(
                child: ListView.builder(
                  itemCount: _symbols.length,
                  itemBuilder: (_, i) {
                    final s = _symbols[i];
                    return ListTile(
                      title: Text(s),
                      selected: _symbol.value == s,
                      onTap: () {
                        _symbol.value = s;
                        coreBridge.postCommand(
                            {'type': 'setSymbol', 'symbol': s, 'req_id': 'sym'});
                        Navigator.of(context).pop();
                        _requestBackfill();
                      },
                    );
                  },
                ),
              ),
              const Divider(),
              const ListTile(title: Text('Timeframe')),
              Padding(
                padding: const EdgeInsets.all(8.0),
                child: DropdownButton<String>(
                  isExpanded: true,
                  value: _timeframe.value,
                  items: _timeframes
                      .map((s) => DropdownMenuItem<String>(
                            value: s,
                            child: Text(s),
                          ))
                      .toList(),
                  onChanged: (v) {
                    if (v != null) {
                      _timeframe.value = v;
                      coreBridge.postCommand({
                        'type': 'setTimeframe',
                        'timeframe': v,
                        'req_id': 'tf'
                      });
                      Navigator.of(context).pop();
                      _requestBackfill();
                    }
                  },
                ),
              ),
            ],
          ),
        ),
      ),
      body: Column(
        children: [
          Expanded(
            child: Stack(
              children: [
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
                      padding: const EdgeInsets.only(bottom: 70),
                      child: cs.Candlesticks(candles: csCandles),
                    );
                  },
                ),
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
                        if (spots.length < 2) return const SizedBox.shrink();
                        return Padding(
                          padding: const EdgeInsets.only(bottom: 70),
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
                Positioned(
                  left: 0,
                  right: 0,
                  bottom: 0,
                  height: 70,
                  child: StreamBuilder<core.AggregatedDataPoint>(
                    stream: coreBridge.aggregatedDataPointStream,
                    builder: (context, snap) {
                      final bars = <BarChartGroupData>[];
                      var i = 0;
                      for (final p in _aggPoints.take(150)) {
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
                          Expanded(child: Text('${m['exchange']}')),
                          Text('~${m['latencyMsEstimate']} ms'),
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

class _StatusDot extends StatelessWidget {
  final bool connected;
  const _StatusDot({required this.connected});
  @override
  Widget build(BuildContext context) {
    return Container(
      width: 10,
      height: 10,
      decoration: BoxDecoration(
        color: connected ? Colors.green : Colors.red,
        shape: BoxShape.circle,
      ),
    );
  }
}
