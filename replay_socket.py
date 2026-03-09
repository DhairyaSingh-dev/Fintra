import logging
import threading
import time
from flask_socketio import Namespace, emit, disconnect
from flask import request
from replay import get_one_min_candles

logger = logging.getLogger(__name__)

class ReplayNamespace(Namespace):
    def __init__(self, namespace):
        super().__init__(namespace)
        self.sessions = {}

    def on_connect(self):
        logger.info(f"Replay socket connected: {request.sid}")
        # Initialise session dict
        self.sessions[request.sid] = {
            'paused': False,
            'speed': 1.0,
            'index': 0,
            'candles': []
        }

    def on_disconnect(self):
        logger.info(f"Replay socket disconnected: {request.sid}")
        self.sessions.pop(request.sid, None)

    def on_init(self, data):
        """Client sends init with symbol, start, end, mode (replay|slideshow)."""
        sid = request.sid
        symbol = data.get('symbol')
        start = data.get('start')
        end = data.get('end')
        mode = data.get('mode', 'replay')
        if not (symbol and start and end):
            emit('error', {'msg': 'missing parameters'}, room=sid)
            disconnect()
            return
        try:
            df = get_one_min_candles(symbol, start, end)
            candles = df.to_dict(orient='records')
            sess = self.sessions.get(sid, {})
            sess.update({
                'candles': candles,
                'index': 0,
                'mode': mode,
                'paused': False,
                'speed': 1.0
            })
            emit('ready', {'total': len(candles)}, room=sid)
        except Exception as e:
            logger.error(f"Replay init error: {e}")
            emit('error', {'msg': str(e)}, room=sid)
            disconnect()

    def _send_next(self, sid):
        sess = self.sessions.get(sid)
        if not sess:
            return
        idx = sess['index']
        if idx >= len(sess['candles']):
            emit('end', {}, room=sid)
            return
        candle = sess['candles'][idx]
        emit('candle', candle, room=sid)
        sess['index'] += 1

    def on_control(self, data):
        sid = request.sid
        sess = self.sessions.get(sid)
        if not sess:
            emit('error', {'msg': 'session not initialized'}, room=sid)
            return
        action = data.get('action')
        if action == 'set_speed':
            try:
                sess['speed'] = float(data.get('speed', 1.0))
            except ValueError:
                sess['speed'] = 1.0
        elif action == 'pause':
            sess['paused'] = True
        elif action == 'resume':
            sess['paused'] = False
        elif action == 'step':
            direction = data.get('direction')
            if direction == 'forward':
                self._send_next(sid)
            elif direction == 'back':
                sess['index'] = max(0, sess['index'] - 2)
                self._send_next(sid)
        # No immediate response needed for other actions

    def on_start(self):
        """Begin automatic streaming (used for replay mode)."""
        sid = request.sid
        sess = self.sessions.get(sid)
        if not sess:
            emit('error', {'msg': 'session not initialized'}, room=sid)
            return
        def stream():
            while True:
                sess = self.sessions.get(sid)
                if not sess:
                    break
                if sess.get('paused'):
                    time.sleep(0.1)
                    continue
                if sess['index'] >= len(sess['candles']):
                    emit('end', {}, room=sid)
                    break
                self._send_next(sid)
                interval = 60.0 / sess.get('speed', 1.0)
                time.sleep(interval)
        threading.Thread(target=stream, daemon=True).start()
