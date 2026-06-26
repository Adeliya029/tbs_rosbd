import urllib.request
import urllib.parse
import json

import api.config as cfg


def send_telegram(message: str) -> bool:
    if not cfg.TELEGRAM_BOT_TOKEN or not cfg.TELEGRAM_CHAT_ID:
        print(f"[TELEGRAM SKIPPED] No token/chat_id configured")
        return False

    url = f"https://api.telegram.org/bot{cfg.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": cfg.TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[TELEGRAM] Alert sent (status={resp.status})")
            return True
    except Exception as e:
        print(f"[TELEGRAM] Failed: {e}")
        return False


RISK_EMOJI = {
    "LOW": "🟢",
    "MEDIUM": "🟡",
    "HIGH": "🟠",
    "CRITICAL": "🔴",
}


def build_alert_message(result: dict) -> str | None:
    risk_class = result.get("risk_class", "").upper()
    risk_prob = result.get("risk_probability", 0)
    anomaly_score = result.get("anomaly_score", 0)
    swarm_density = result.get("swarm_density", 0)
    region = result.get("region", "Unknown")
    magnitude = result.get("magnitude", 0)
    depth_km = result.get("depth_km", 0)
    quake_count = result.get("quake_count_24h", 0)

    alerts = []

    if risk_class == "CRITICAL":
        alerts.append(
            f"🚨 <b>CRITICAL RISK</b> 🚨\n"
            f"Region   : {region}\n"
            f"Risk     : {risk_class} ({risk_prob:.1%})\n"
            f"Mag      : M{magnitude:.1f}\n"
            f"Depth    : {depth_km:.0f} km\n"
            f"24h Count: {quake_count}"
        )

    if anomaly_score >= cfg.ANOMALY_WARNING_THRESHOLD:
        alerts.append(
            f"⚠️ <b>ANOMALY DETECTED</b> ⚠️\n"
            f"Region       : {region}\n"
            f"Anomaly Score: {anomaly_score:.3f}\n"
            f"Mag          : M{magnitude:.1f}"
        )

    if swarm_density > 0:
        alerts.append(
            f"🔔 <b>SWARM ACTIVE</b> 🔔\n"
            f"Region      : {region}\n"
            f"Swarm Density: {swarm_density:.1f}"
        )

    if not alerts:
        return None

    return "\n\n─────────────\n\n".join(alerts)


def trigger_alerts(result: dict) -> list[str]:
    message = build_alert_message(result)
    if not message:
        return []

    header = f"<b>🌍 TBS-ROSBD Alert</b>\n{RISK_EMOJI.get(result.get('risk_class', '').upper(), '⚪')}\n\n"
    full_message = header + message

    sent = send_telegram(full_message)
    return [full_message] if sent else []
