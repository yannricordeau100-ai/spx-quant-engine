"""
Notificateurs — Telegram + Resend (email) + log fichier.

Usage :
    from notifiers import send_all
    send_all(subject="BBE alert", body="ticker XYZ ...", markdown=True)

Les clés API sont lues depuis .telegram_token / .telegram_chat_id / .resend_api_key
(tous gitignored). Si une clé manque, le canal correspondant est skippé sans erreur.
"""
from __future__ import annotations
import datetime as _dt
from pathlib import Path
import requests

BASE = Path(__file__).parent
LOG_FILE = BASE / "alerts.log"


def _read(file: str) -> str | None:
    p = BASE / file
    if p.exists():
        t = p.read_text().strip()
        return t if t else None
    return None


def send_telegram(text: str, markdown: bool = True) -> bool:
    token = _read(".telegram_token")
    chat_id = _read(".telegram_chat_id")
    if not token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown" if markdown else None,
                "disable_web_page_preview": "true",
            },
            timeout=15,
        ).json()
        return bool(r.get("ok"))
    except Exception as e:
        _log(f"[telegram ERROR] {e}")
        return False


def send_email(
    subject: str,
    html_body: str,
    to: str | None = None,
) -> bool:
    api_key = _read(".resend_api_key")
    to = to or _read(".resend_to_email")
    if not api_key or not to:
        return False
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": "BBE Scanner <onboarding@resend.dev>",
                "to": [to],
                "subject": subject,
                "html": html_body,
            },
            timeout=15,
        )
        return r.status_code in (200, 201, 202)
    except Exception as e:
        _log(f"[resend ERROR] {e}")
        return False


def _log(line: str) -> None:
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with LOG_FILE.open("a") as f:
        f.write(f"[{ts}] {line}\n")


def send_all(
    subject: str,
    markdown_body: str,
    html_body: str | None = None,
) -> dict:
    """Envoie sur tous les canaux. Retourne un dict {channel: success}."""
    result = {
        "telegram": send_telegram(markdown_body),
        "email": send_email(subject, html_body or markdown_body.replace("\n", "<br>")),
    }
    _log(f"send_all subject={subject!r} result={result}")
    return result


if __name__ == "__main__":
    # Test rapide : python3 notifiers.py
    res = send_all(
        "BBE Scanner — Test",
        "🧪 *Test notifiers.py*\n\nSi tu lis ça, les 2 canaux sont OK.",
        "<h2>🧪 Test notifiers.py</h2><p>Si tu lis ça, les 2 canaux sont OK.</p>",
    )
    print(res)
