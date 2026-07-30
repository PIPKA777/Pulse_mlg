#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сервер для удалённого управления кардиограммой.

Технологии:
- Flask — лёгкий веб-фреймворк
- Server-Sent Events (SSE) — для отправки команд на страницу в реальном времени

Запуск:
    pip install flask
    python app.py
    Открыть http://localhost:5000

API:
    GET  /                          — отдаёт index.html
    GET  /events                    — SSE-поток (клиент подписывается на изменения)
    POST /set_speed                 — установить скорость (JSON: {"value": 2.5})
    GET  /set_speed?value=2.5       — установить скорость через query-параметр
"""

import json
import time
import logging
from flask import Flask, request, Response, send_from_directory

app = Flask(__name__, static_folder='.')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
log = logging.getLogger(__name__)

# Текущая скорость (базовая)
current_speed = 1.5

# Список клиентов, подключённых через SSE
# Каждый элемент — это queue.Queue, в которую кладём сообщения
sse_clients = []


# ============================================================
# 1. Отдача статики
# ============================================================
@app.route('/')
def index():
    """Главная страница — index.html"""
    return send_from_directory('.', 'index.html')


# ============================================================
# 2. SSE-эндпоинт
# ============================================================
@app.route('/events')
def sse_stream():
    """
    Server-Sent Events поток.
    Клиент подключается и получает все изменения скорости.
    """
    def generate():
        # Создаём очередь для этого клиента
        from queue import Queue
        q = Queue()
        sse_clients.append(q)
        log.info('SSE client connected (total: %d)', len(sse_clients))
        try:
            # Сразу отправляем текущее значение
            yield f"data: {json.dumps({'type': 'speed', 'value': current_speed})}\n\n"
            while True:
                # Ждём новое сообщение (с таймаутом для проверки соединения)
                try:
                    msg = q.get(timeout=30)
                    yield f"data: {msg}\n\n"
                except Exception:
                    # Таймаут — отправляем keepalive
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            # При отключении удаляем очередь
            if q in sse_clients:
                sse_clients.remove(q)
            log.info('SSE client disconnected (total: %d)', len(sse_clients))

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
        }
    )


# ============================================================
# 3. Управление скоростью
# ============================================================
def broadcast_speed(value):
    """Разослать новое значение скорости всем подключённым SSE-клиентам"""
    msg = json.dumps({'type': 'speed', 'value': value})
    dead_clients = []
    for q in sse_clients:
        try:
            q.put_nowait(msg)
        except Exception:
            dead_clients.append(q)
    # Удаляем мёртвые соединения
    for q in dead_clients:
        if q in sse_clients:
            sse_clients.remove(q)
    log.info('Broadcast speed=%.1f to %d clients', value, len(sse_clients))


@app.route('/set_speed', methods=['GET', 'POST'])
def set_speed():
    """
    Установить скорость.
    GET:  /set_speed?value=2.5
    POST: {"value": 2.5}
    """
    global current_speed

    # Извлекаем значение
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        value = data.get('value')
    else:
        value = request.args.get('value')

    if value is None:
        return json.dumps({'error': 'Missing "value" parameter'}), 400, {
            'Content-Type': 'application/json'
        }

    try:
        val = float(value)
    except (ValueError, TypeError):
        return json.dumps({'error': f'Invalid value: {value}'}), 400, {
            'Content-Type': 'application/json'
        }

    # Ограничиваем диапазон
    val = max(0.2, min(5.0, val))
    current_speed = val

    # Оповещаем всех клиентов
    broadcast_speed(val)

    log.info('Speed set to %.1f', val)
    return json.dumps({'status': 'ok', 'value': val}), 200, {
        'Content-Type': 'application/json'
    }


# ============================================================
# 4. Текущее состояние (для проверки)
# ============================================================
@app.route('/status')
def status():
    """Возвращает текущее состояние сервера"""
    return json.dumps({
        'speed': current_speed,
        'clients': len(sse_clients),
    }), 200, {'Content-Type': 'application/json'}


# ============================================================
# 5. Запуск
# ============================================================
if __name__ == '__main__':
    log.info('Starting ECG server on http://localhost:5000')
    log.info('  API:')
    log.info('    GET /set_speed?value=1.5  — установить скорость')
    log.info('    POST /set_speed           — JSON: {"value": 2.5}')
    log.info('    GET /status               — состояние сервера')
    log.info('  Press Ctrl+C to stop.')
    # threaded=True — поддержка нескольких SSE-клиентов
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)