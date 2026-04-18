from __future__ import annotations

import base64
from urllib.parse import unquote_plus

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from db import get_conn, init_db

app = FastAPI(title='Kocka Birodalom Hírlevél Tracker')
PIXEL_GIF = base64.b64decode('R0lGODlhAQABAIABAP///wAAACwAAAAAAQABAAACAkQBADs=')


@app.on_event('startup')
def startup() -> None:
    init_db()


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}


def _record(send_id: str, campaign_id: str, recipient_email: str, event_type: str, request: Request, url: str = '') -> None:
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO events(send_id, campaign_id, recipient_email, event_type, url, user_agent, ip_address) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (
                unquote_plus(send_id),
                unquote_plus(campaign_id or ''),
                unquote_plus(recipient_email or '').lower(),
                event_type,
                unquote_plus(url or ''),
                request.headers.get('user-agent', ''),
                request.client.host if request.client else '',
            ),
        )
        conn.commit()


@app.get('/o/{send_id}.gif')
async def open_pixel(send_id: str, request: Request, c: str = '', e: str = '') -> Response:
    _record(send_id, c, e, 'open', request)
    return Response(content=PIXEL_GIF, media_type='image/gif')


@app.get('/c/{send_id}')
async def click_redirect(send_id: str, request: Request, u: str, c: str = '', e: str = '') -> RedirectResponse:
    destination = unquote_plus(u)
    _record(send_id, c, e, 'click', request, url=destination)
    return RedirectResponse(destination)


@app.get('/u/{send_id}')
async def unsubscribe(send_id: str, request: Request, c: str = '', e: str = '') -> HTMLResponse:
    _record(send_id, c, e, 'unsubscribe', request)
    return HTMLResponse('<html><body style="font-family:Arial,sans-serif;padding:32px;"><h2>Sikeres leiratkozás</h2><p>Leiratkoztál a Kocka Birodalom hírleveléről.</p></body></html>')


@app.get('/stats')
def stats() -> dict:
    with get_conn() as conn:
        per_send_rows = conn.execute(
            """
            SELECT send_id,
                   SUM(CASE WHEN event_type = 'open' THEN 1 ELSE 0 END) AS opens,
                   SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
                   SUM(CASE WHEN event_type = 'unsubscribe' THEN 1 ELSE 0 END) AS unsubscribes
            FROM events
            GROUP BY send_id
            """
        ).fetchall()
        link_rows = conn.execute(
            """
            SELECT campaign_id, url, COUNT(*) AS clicks
            FROM events
            WHERE event_type = 'click' AND url <> ''
            GROUP BY campaign_id, url
            ORDER BY clicks DESC, campaign_id DESC
            """
        ).fetchall()
    return {
        'per_send': {
            row['send_id']: {
                'opens': row['opens'] or 0,
                'clicks': row['clicks'] or 0,
                'unsubscribes': row['unsubscribes'] or 0,
            }
            for row in per_send_rows
        },
        'links': [
            {
                'campaign_id': row['campaign_id'],
                'url': row['url'],
                'clicks': row['clicks'],
            }
            for row in link_rows
        ],
    }


@app.get('/unsubscribed')
def unsubscribed() -> dict:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT recipient_email
            FROM events
            WHERE event_type = 'unsubscribe' AND recipient_email <> ''
            ORDER BY recipient_email
            """
        ).fetchall()
    return {'emails': [r['recipient_email'] for r in rows]}
