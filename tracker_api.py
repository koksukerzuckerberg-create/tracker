from __future__ import annotations

import base64
from collections import defaultdict
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


def _norm(v: str | None) -> str:
    return unquote_plus(v or '').strip()


def _record(send_id: str, campaign_id: str, recipient_email: str, event_type: str, request: Request, url: str = '') -> None:
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO events(send_id, campaign_id, recipient_email, event_type, url, user_agent, ip_address) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (
                _norm(send_id),
                _norm(campaign_id),
                _norm(recipient_email).lower(),
                event_type,
                _norm(url),
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
async def click_redirect(send_id: str, request: Request, u: str = '', url: str = '', c: str = '', e: str = '') -> RedirectResponse:
    destination = _norm(u or url)
    if not destination:
        destination = 'https://kockabirodalom.hu'
    _record(send_id, c, e, 'click', request, url=destination)
    return RedirectResponse(destination)


@app.get('/u/{send_id}')
async def unsubscribe(send_id: str, request: Request, c: str = '', e: str = '') -> HTMLResponse:
    _record(send_id, c, e, 'unsubscribe', request)
    return HTMLResponse(
        '<html><body style="font-family:Arial,sans-serif;padding:32px;background:#fff8d6;color:#113a7d;">'
        '<img src="https://kockabirodalom.hu/site/ui/images/logo.png" alt="Kocka Birodalom" style="max-width:280px;display:block;margin-bottom:18px;">'
        '<h2>Sikeres leiratkozás</h2><p>Leiratkoztál a Kocka Birodalom hírleveléről.</p></body></html>'
    )


@app.get('/stats')
def stats() -> dict:
    with get_conn() as conn:
        rows = conn.execute(
            '''
            SELECT send_id, campaign_id, recipient_email, event_type, url, created_at
            FROM events
            ORDER BY created_at DESC, id DESC
            '''
        ).fetchall()

    per_send: dict[str, dict] = defaultdict(lambda: {'opens': 0, 'clicks': 0, 'unsubscribes': 0})
    campaigns: dict[str, dict] = defaultdict(lambda: {
        'campaign_id': '',
        'opens': 0,
        'clicks': 0,
        'unsubscribes': 0,
        'unique_openers': set(),
        'unique_clickers': set(),
        'unique_recipients': set(),
    })
    links: dict[tuple[str, str], dict] = defaultdict(lambda: {'campaign_id': '', 'url': '', 'clicks': 0, 'unique_clickers': set()})
    recipients: dict[tuple[str, str], dict] = defaultdict(lambda: {
        'campaign_id': '',
        'recipient_email': '',
        'opens': 0,
        'clicks': 0,
        'unsubscribed': 0,
        'last_url': '',
    })

    total_opens = total_clicks = total_unsubs = 0
    unique_openers = set()
    unique_clickers = set()
    recent_events = []

    for row in rows:
        send_id = row['send_id'] or ''
        campaign_id = row['campaign_id'] or ''
        email = (row['recipient_email'] or '').lower()
        event_type = row['event_type'] or ''
        url = row['url'] or ''

        ps = per_send[send_id]
        camp = campaigns[campaign_id]
        camp['campaign_id'] = campaign_id
        if email:
            camp['unique_recipients'].add(email)
        rec = recipients[(campaign_id, email)]
        rec['campaign_id'] = campaign_id
        rec['recipient_email'] = email

        if event_type == 'open':
            ps['opens'] += 1
            camp['opens'] += 1
            rec['opens'] += 1
            total_opens += 1
            if email:
                camp['unique_openers'].add(email)
                unique_openers.add(email)
        elif event_type == 'click':
            ps['clicks'] += 1
            camp['clicks'] += 1
            rec['clicks'] += 1
            rec['last_url'] = url
            total_clicks += 1
            if email:
                camp['unique_clickers'].add(email)
                unique_clickers.add(email)
            lk = links[(campaign_id, url)]
            lk['campaign_id'] = campaign_id
            lk['url'] = url
            lk['clicks'] += 1
            if email:
                lk['unique_clickers'].add(email)
        elif event_type == 'unsubscribe':
            ps['unsubscribes'] += 1
            camp['unsubscribes'] += 1
            rec['unsubscribed'] = 1
            total_unsubs += 1

        if len(recent_events) < 100:
            recent_events.append({
                'campaign_id': campaign_id,
                'recipient_email': email,
                'event_type': event_type,
                'url': url,
                'created_at': row['created_at'],
            })

    return {
        'totals': {
            'opens': total_opens,
            'clicks': total_clicks,
            'unsubscribes': total_unsubs,
            'unique_openers': len(unique_openers),
            'unique_clickers': len(unique_clickers),
            'campaigns': len(campaigns),
        },
        'per_send': dict(per_send),
        'campaigns': [
            {
                'campaign_id': camp['campaign_id'],
                'opens': camp['opens'],
                'clicks': camp['clicks'],
                'unsubscribes': camp['unsubscribes'],
                'unique_openers': len(camp['unique_openers']),
                'unique_clickers': len(camp['unique_clickers']),
                'unique_recipients': len(camp['unique_recipients']),
            }
            for camp in sorted(campaigns.values(), key=lambda x: int(x['campaign_id'] or 0), reverse=True)
        ],
        'links': [
            {
                'campaign_id': lk['campaign_id'],
                'url': lk['url'],
                'clicks': lk['clicks'],
                'unique_clickers': len(lk['unique_clickers']),
            }
            for lk in sorted(links.values(), key=lambda x: (x['clicks'], x['campaign_id']), reverse=True)
        ],
        'recipients': [
            rec
            for rec in sorted(recipients.values(), key=lambda x: (x['campaign_id'], x['clicks'], x['opens']), reverse=True)
        ],
        'recent_events': recent_events,
    }


@app.get('/unsubscribed')
def unsubscribed() -> dict:
    with get_conn() as conn:
        rows = conn.execute(
            '''
            SELECT DISTINCT recipient_email
            FROM events
            WHERE event_type = 'unsubscribe' AND recipient_email <> ''
            ORDER BY recipient_email
            '''
        ).fetchall()
    return {'emails': [r['recipient_email'] for r in rows]}
