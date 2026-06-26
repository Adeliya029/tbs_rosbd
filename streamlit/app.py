# -*- coding: utf-8 -*-
import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="TBS-ROSBD", layout="wide")

st.markdown("""<meta http-equiv="refresh" content="30">""", unsafe_allow_html=True)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "seismic_db")
PG_USER = os.getenv("PG_USER", "rosbd")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rosbd123")

RISK_LABEL = {"LOW": "RENDAH", "MEDIUM": "WASPADA", "HIGH": "TINGGI", "CRITICAL": "KRITIS"}
RISK_EMOJI = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}
RISK_LEVEL = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
RISK_COLOR = {"LOW": "#28b463", "MEDIUM": "#f39c12", "HIGH": "#e67e22", "CRITICAL": "#e74c3c"}


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=PG_PASSWORD,
    )


def load_latest():
    conn = get_conn()
    try:
        df = pd.read_sql("""
            SELECT DISTINCT ON (region) id, region, latitude, longitude,
                   magnitude, depth_km, risk_class, event_time, created_at
            FROM risk_scores
            ORDER BY region, created_at DESC
        """, conn)
        return df
    finally:
        conn.close()


df = load_latest()

now = datetime.now(timezone.utc)
max_level = max(RISK_LEVEL.get(r, 0) for r in df["risk_class"]) if not df.empty else 0
has_critical = max_level >= 3
has_high = max_level >= 2

latest_row = df.sort_values("event_time", ascending=False).iloc[0] if not df.empty else None
total_wilayah = len(df)
berisiko = len(df[df["risk_class"].isin(["HIGH", "CRITICAL"])])

# -- CSS --
st.markdown("""
<style>
    .header {
        background: linear-gradient(135deg, #0d47a1, #1565c0);
        padding: 20px 30px;
        border-radius: 0 0 16px 16px;
        color: white;
        margin: -60px -60px 20px -60px;
    }
    .header h1 { font-size: 28px; margin: 0; font-weight: 700; }
    .header p { font-size: 14px; margin: 4px 0 0 0; opacity: 0.85; }
    .status-banner {
        padding: 16px 24px;
        border-radius: 12px;
        font-size: 22px;
        font-weight: 700;
        margin: 0 0 20px 0;
        text-align: center;
    }
    .status-banner small {
        font-weight: 400;
        font-size: 14px;
        display: block;
        margin-top: 4px;
    }
    .hero-card {
        background: white;
        border-radius: 14px;
        padding: 24px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        margin-bottom: 20px;
        border-left: 6px solid #0d47a1;
    }
    .hero-card .mag {
        font-size: 48px;
        font-weight: 800;
        line-height: 1;
    }
    .hero-card .label {
        color: #666;
        font-size: 13px;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .hero-card .value {
        font-size: 18px;
        font-weight: 600;
    }
    .stat-card {
        background: white;
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        margin-bottom: 20px;
    }
    .stat-card .num {
        font-size: 32px;
        font-weight: 800;
        color: #0d47a1;
    }
    .stat-card .lbl {
        font-size: 12px;
        color: #888;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .risk-badge {
        display: inline-block;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 13px;
        font-weight: 700;
        color: white;
    }
    .recommend-box {
        background: #e8f5e9;
        border-radius: 12px;
        padding: 20px 24px;
        margin-top: 16px;
    }
    .recommend-box.warn {
        background: #fff3e0;
    }
    .recommend-box.danger {
        background: #ffebee;
    }
    .recommend-box h3 {
        margin: 0 0 12px 0;
        font-size: 16px;
    }
    .recommend-box li {
        margin-bottom: 6px;
        font-size: 15px;
    }
    .footer {
        text-align: center;
        padding: 20px;
        color: #999;
        font-size: 13px;
        margin-top: 30px;
    }
    .table-header {
        font-size: 18px;
        font-weight: 700;
        margin-bottom: 12px;
        color: #0d47a1;
    }
</style>
""", unsafe_allow_html=True)

# -- HEADER --
st.markdown(f"""
<div class="header">
    <h1>🌏 SISTEM INFORMASI GEMPABUMI</h1>
    <p>TBS-ROSBD | Big Data Engineering — Realtime | {now.strftime('%d %b %Y %H:%M')} WIB</p>
</div>
""", unsafe_allow_html=True)

# -- STATUS BANNER --
if has_critical:
    st.markdown(f"""
    <div class="status-banner" style="background:#ffebee;color:#c62828;border:2px solid #ef9a9a;">
        🔴 STATUS: BAHAYA
        <small>Ada wilayah dengan tingkat KRITIS. Harap waspada dan ikuti arahan.</small>
    </div>
    """, unsafe_allow_html=True)
elif has_high:
    st.markdown(f"""
    <div class="status-banner" style="background:#fff3e0;color:#e65100;border:2px solid #ffcc80;">
        🟡 STATUS: WASPADA
        <small>Ada wilayah dengan tingkat TINGGI. Pantau perkembangan.</small>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div class="status-banner" style="background:#e8f5e9;color:#2e7d32;border:2px solid #a5d6a7;">
        🟢 STATUS: AMAN
        <small>Seluruh wilayah dalam kondisi aman.</small>
    </div>
    """, unsafe_allow_html=True)

# -- HERO CARD: GEMPABUMI TERKINI --
if latest_row is not None:
    mag = latest_row["magnitude"]
    depth = latest_row["depth_km"]
    region = latest_row["region"]
    rclass = latest_row["risk_class"]
    etime = pd.to_datetime(latest_row["event_time"]).strftime('%d %b %Y %H:%M')
    label = RISK_LABEL.get(rclass, rclass)
    emoji = RISK_EMOJI.get(rclass, "⚪")
    color = RISK_COLOR.get(rclass, "#666")

    st.markdown(f"""
    <div class="hero-card">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <div class="label">GEMPABUMI TERKINI</div>
                <div style="display:flex;align-items:baseline;gap:16px;margin-top:8px;">
                    <div><span class="mag">{mag:.1f}</span> <span style="font-size:20px;font-weight:600;color:#666;">M</span></div>
                    <div style="border-left:2px solid #eee;padding-left:16px;">
                        <div class="label">Kedalaman</div>
                        <div class="value">{depth:.0f} km</div>
                    </div>
                    <div style="border-left:2px solid #eee;padding-left:16px;">
                        <div class="label">Waktu</div>
                        <div class="value">{etime}</div>
                    </div>
                </div>
            </div>
            <div style="text-align:center;">
                <div style="font-size:40px;">{emoji}</div>
                <div class="risk-badge" style="background:{color};">{label}</div>
            </div>
        </div>
        <div style="margin-top:14px;padding-top:14px;border-top:1px solid #f0f0f0;">
            <div class="label">Lokasi</div>
            <div class="value">{region}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# -- STATISTIK --
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"""
    <div class="stat-card">
        <div class="num">{total_wilayah}</div>
        <div class="lbl">Wilayah Terpantau</div>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="stat-card">
        <div class="num" style="color:{'#e74c3c' if berisiko > 0 else '#28b463'};">{berisiko}</div>
        <div class="lbl">Berisiko Tinggi / Kritis</div>
    </div>
    """, unsafe_allow_html=True)
with col3:
    update_time = pd.to_datetime(df["created_at"].max()).strftime('%H:%M') if not df.empty else "-"
    st.markdown(f"""
    <div class="stat-card">
        <div class="num" style="font-size:24px;">{update_time}</div>
        <div class="lbl">Update Terakhir</div>
    </div>
    """, unsafe_allow_html=True)

# -- MAP --
st.markdown("<div style='margin-top:4px;'>", unsafe_allow_html=True)
st.markdown("<div class='table-header'>🗺️ PETA SEBARAN GEMPABUMI</div>", unsafe_allow_html=True)

df_map = df.dropna(subset=["latitude", "longitude"]).copy()
if not df_map.empty:
    df_map["color"] = df_map["risk_class"].map(RISK_COLOR).fillna("#666")
    df_map["size"] = df_map["magnitude"] * 4 + 5
    try:
        import folium
        from streamlit_folium import st_folium

        center_lat = df_map["latitude"].mean()
        center_lon = df_map["longitude"].mean()
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=5,
            tiles="OpenStreetMap",
            control_scale=True,
        )

        for _, row in df_map.iterrows():
            label = RISK_LABEL.get(row["risk_class"], row["risk_class"])
            popup = folium.Popup(f"""
                <b>{row['region'][:60]}</b><br>
                <span style='color:{row["color"]};font-weight:700;'>{label}</span><br>
                Mag: M{row['magnitude']:.1f} | Kedalaman: {row['depth_km']:.0f} km<br>
                {pd.to_datetime(row['event_time']).strftime('%d %b %H:%M')}
            """, max_width=280)
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=row["size"],
                color=row["color"],
                fill=True,
                fill_opacity=0.7,
                fill_color=row["color"],
                popup=popup,
                weight=2,
            ).add_to(m)

        st_folium(m, width=None, height=500, returned_objects=[])
    except ImportError:
        st.warning("Install folium & streamlit-folium untuk peta interaktif.")
        st.dataframe(df_map[["region", "risk_class", "magnitude", "latitude", "longitude"]],
                     use_container_width=True)
else:
    st.info("Data koordinat belum tersedia.")

# -- TABLE --
st.markdown("<div style='margin-top:24px;'>", unsafe_allow_html=True)
st.markdown("<div class='table-header'>📋 DAFTAR GEMPABUMI TERKINI</div>", unsafe_allow_html=True)

if not df.empty:
    display = df[["region", "magnitude", "depth_km", "risk_class", "event_time"]].copy()
    display["Tingkat"] = display["risk_class"].map(lambda r: f"{RISK_EMOJI.get(r, '')} {RISK_LABEL.get(r, r)}")
    display["Magnitudo"] = display["magnitude"].apply(lambda x: f"M{x:.1f}")
    display["Kedalaman"] = display["depth_km"].apply(lambda x: f"{x:.0f} km")
    display["Waktu"] = pd.to_datetime(display["event_time"]).dt.strftime('%d %b %H:%M')
    display["Wilayah"] = display["region"].str[:50]

    st.dataframe(
        display[["Wilayah", "Magnitudo", "Kedalaman", "Tingkat", "Waktu"]].sort_values("Waktu", ascending=False),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Wilayah": st.column_config.TextColumn("Wilayah"),
            "Magnitudo": st.column_config.TextColumn("Mag", width="small"),
            "Kedalaman": st.column_config.TextColumn("Kedalaman", width="small"),
            "Tingkat": st.column_config.TextColumn("Tingkat Bahaya"),
            "Waktu": st.column_config.TextColumn("Waktu"),
        },
    )
else:
    st.info("Belum ada data gempa.")

# -- REKOMENDASI --
if has_critical:
    st.markdown("""
    <div class="recommend-box danger">
        <h3>🚨 YANG HARUS DILAKUKAN</h3>
        <ul>
            <li>✅ Tetap tenang dan jangan panik</li>
            <li>✅ Segera menjauh dari pantai jika berada di zona bahaya tsunami</li>
            <li>✅ Ikuti arahan dari BPBD dan petugas setempat</li>
            <li>✅ Cari informasi dari sumber resmi BMKG</li>
            <li>📞 Hubungi darurat: <b>112</b> atau BPBD setempat</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
elif has_high:
    st.markdown("""
    <div class="recommend-box warn">
        <h3>⚠️ YANG HARUS DILAKUKAN</h3>
        <ul>
            <li>✅ Tetap tenang dan pantau informasi</li>
            <li>✅ Siapkan tas siaga bencana (obat, makanan, dokumen penting)</li>
            <li>✅ Kenali jalur evakuasi terdekat dari rumah Anda</li>
            <li>✅ Ikuti informasi dari BMKG dan BPBD</li>
            <li>📞 Hubungi <b>112</b> jika melihat kondisi darurat</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="recommend-box">
        <h3>✅ YANG HARUS DILAKUKAN</h3>
        <ul>
            <li>✅ Tetap tenang dan selalu siaga</li>
            <li>✅ Pelajari jalur evakuasi di wilayah Anda</li>
            <li>✅ Siapkan perlengkapan darurat di rumah</li>
            <li>✅ Pantau informasi BMKG secara berkala</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

# -- FOOTER --
st.markdown("""
<div class="footer">
    TBS-ROSBD — Sistem Informasi Kebencanaan Berbasis Big Data<br>
    Data bersumber dari pipeline real-time | © 2026
</div>
""", unsafe_allow_html=True)
