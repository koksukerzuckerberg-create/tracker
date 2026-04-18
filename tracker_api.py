from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, Response
from db import get_conn, init_db

app = FastAPI()

init_db()

@app.get("/open/{send_id}.png")
async def track_open(send_id: str, request: Request):
    conn = get_conn()
    conn.execute(
        "INSERT INTO events (send_id, event_type, user_agent, ip_address) VALUES (?, ?, ?, ?)",
        (
            send_id,
            "open",
            request.headers.get("user-agent"),
            request.client.host,
        ),
    )
    conn.commit()
    conn.close()

    return Response(
        content=b"\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff\x21\xf9\x04\x01\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3b",
        media_type="image/gif",
    )

@app.get("/click")
async def track_click(send_id: str, redirect: str, request: Request):
    conn = get_conn()
    conn.execute(
        "INSERT INTO events (send_id, event_type, url, user_agent, ip_address) VALUES (?, ?, ?, ?, ?)",
        (
            send_id,
            "click",
            redirect,
            request.headers.get("user-agent"),
            request.client.host,
        ),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url=redirect)
