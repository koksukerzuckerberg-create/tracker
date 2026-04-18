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

    return Response(content=b"", media_type="image/gif")

@app.get("/c/{send_id}")
async def track_click(send_id: str, request: Request):
    params = request.query_params

    url = params.get("u") or params.get("url")
    campaign = params.get("c")
    email = params.get("e")

    if not url:
        return {"error": "no url"}

    conn = get_conn()
    conn.execute(
        "INSERT INTO events (send_id, event_type, url, user_agent, ip_address) VALUES (?, ?, ?, ?, ?)",
        (
            send_id,
            "click",
            url,
            request.headers.get("user-agent"),
            request.client.host,
        ),
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url=url)
