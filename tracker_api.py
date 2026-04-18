from __future__ import annotations

import base64
from urllib.parse import unquote_plus

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, Response

from .db import get_conn, init_db

app = FastAPI(title='Newsletter Pro Tracker')

PIXEL_GIF = base64.b64decode(
    'R0lGODlhAQABAIABAP///wAAACwAAAAAAQABAAACAkQBADs='
)


@app.on_event('startup')
def startup() -> None:
    init_db()


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}


@app.get('/o/{send_id}.gif')
async def open_pixel(send_id: str, request: Request) -> Response:
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO events(send_id, event_type, user_agent, ip_address) VALUES (?, ?, ?, ?)',
            (
                unquote_plus(send_id),
                'open',
                request.headers.get('user-agent', ''),
                request.client.host if request.client else '',
            ),
        )
    return Response(content=PIXEL_GIF, media_type='image/gif')


@app.get('/c/{send_id}')
async def click_redirect(send_id: str, url: str, request: Request) -> RedirectResponse:
    destination = unquote_plus(url)
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO events(send_id, event_type, url, user_agent, ip_address) VALUES (?, ?, ?, ?, ?)',
            (
                unquote_plus(send_id),
                'click',
                destination,
                request.headers.get('user-agent', ''),
                request.client.host if request.client else '',
            ),
        )
    return RedirectResponse(destination)
