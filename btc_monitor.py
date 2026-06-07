"""
₿ BTC価格モニター & 通知アプリ
Streamlit + Binance API（無料・APIキー不要）

【使い方】
  pip install streamlit requests pandas plotly schedule apscheduler

  streamlit run btc_monitor.py

【通知方法】
  - メール : Gmail の「アプリパスワード」を使用（2段階認証が必要）
  - Discord: サーバーの Webhook URL を登録するだけ

【データソース】
  Binance Public API（登録不要・無料）
  https://api.binance.com
"""

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import json
import smtplib
import threading
import time
import os
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from apscheduler.schedulers.background import BackgroundScheduler

# ─────────────────────────────────────────
# ページ設定
# ─────────────────────────────────────────
st.set_page_config(
    page_title="BTC Price Monitor",
    page_icon="₿",
    layout="wide",
)

st.markdown("""
<style>
  .stApp { background: linear-gradient(150deg, #0a0f1e 0%, #0d1f35 60%, #0a1020 100%); color: #e8f0fe; }
  .block-container { padding-top: 1.5rem; }
  h1, h2, h3 { color: #f0b429 !important; }
  label { color: #a0b4cc !important; }
  div[data-testid="stMetricValue"] { color: #f0b429 !important; font-size: 1.5rem !important; }
  .up   { color: #4ade80 !important; font-weight: 700; }
  .down { color: #f87171 !important; font-weight: 700; }
  .card {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 14px;
    padding: 1.2rem 1.5rem;
    margin: 0.5rem 0;
  }
  .badge-up   { background:#14532d; color:#4ade80; border-radius:6px; padding:2px 8px; font-weight:700; }
  .badge-down { background:#4c0519; color:#f87171; border-radius:6px; padding:2px 8px; font-weight:700; }
  .badge-flat { background:#1e3a5f; color:#93c5fd; border-radius:6px; padding:2px 8px; font-weight:700; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# 定数
# ─────────────────────────────────────────
DATA_FILE       = "btc_price_history.json"   # 価格履歴の保存先
SETTINGS_FILE   = "btc_settings.json"        # 通知設定の保存先
FETCH_INTERVAL  = 3600                       # 価格取得間隔（秒）= 1時間
NOTIFY_INTERVAL = 6                          # 通知間隔（時間）

# Binance API エンドポイント（無料・登録不要）
BINANCE_TICKER  = "https://api.binance.com/api/v3/ticker/price"


# ─────────────────────────────────────────
# データ管理
# ─────────────────────────────────────────

def load_history() -> list[dict]:
    """保存済みの価格履歴を読み込む"""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return []


def save_history(history: list[dict]):
    """価格履歴をJSONファイルに保存する"""
    # 最大 30日分（720件）だけ保持してファイルサイズを抑える
    with open(DATA_FILE, "w") as f:
        json.dump(history[-720:], f)


def load_settings() -> dict:
    """通知設定を読み込む"""
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            return json.load(f)
    return {
        "emails":          [],   # 通知先メールアドレスのリスト
        "discord_webhooks": [],  # Discord Webhook URLのリスト
        "smtp_host":       "smtp.gmail.com",
        "smtp_port":       587,
        "smtp_user":       "",   # 送信元Gmailアドレス
        "smtp_password":   "",   # Gmailアプリパスワード
    }


def save_settings(settings: dict):
    """通知設定を保存する"""
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f)


# ─────────────────────────────────────────
# Binance API から価格取得
# ─────────────────────────────────────────

def fetch_btc_price() -> dict | None:
    """
    Binance Public APIからBTC/USDT・BTC/JPYの現在価格を取得する。
    BTC/JPYはBTC/USDT × USD/JPYで計算（Binanceに直接BTC/JPYがないため）。

    Returns
    -------
    dict | None
        {"usd": float, "jpy": float, "timestamp": str}
    """
    try:
        # BTC/USDT 価格取得
        r_btc = requests.get(BINANCE_TICKER, params={"symbol": "BTCUSDT"}, timeout=10)
        r_btc.raise_for_status()
        btc_usd = float(r_btc.json()["price"])

        # USD/JPY 取得（USDT建てのJPYペアで代用）
        r_jpy = requests.get(BINANCE_TICKER, params={"symbol": "USDCJPY"}, timeout=10)
        if r_jpy.status_code == 200:
            usd_jpy = float(r_jpy.json()["price"])
        else:
            # フォールバック: 固定レート（実際には別APIを使うのが理想）
            usd_jpy = 155.0

        btc_jpy = btc_usd * usd_jpy

        return {
            "usd"      : round(btc_usd, 2),
            "jpy"      : round(btc_jpy, 0),
            "usd_jpy"  : round(usd_jpy, 2),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"[BTC Fetch Error] {e}")
        return None


# ─────────────────────────────────────────
# Binance 日足 OHLC 取得
# ─────────────────────────────────────────

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

@st.cache_data(ttl=300)  # 5分キャッシュ
def fetch_daily_ohlc(days: int = 30) -> pd.DataFrame:
    """
    Binance から BTC/USDT の日足 OHLC（始値・高値・安値・終値・出来高）を取得する。

    Parameters
    ----------
    days : int
        取得する日数（最大 30日）

    Returns
    -------
    pd.DataFrame
        columns: date, open, high, low, close, volume
    """
    try:
        params = {
            "symbol"  : "BTCUSDT",
            "interval": "1d",          # 日足
            "limit"   : days,          # 取得件数
        }
        r = requests.get(BINANCE_KLINES, params=params, timeout=10)
        r.raise_for_status()
        raw = r.json()

        # Binance Kline レスポンス形式:
        # [開始時刻ms, 始値, 高値, 安値, 終値, 出来高, 終了時刻ms, ...]
        rows = []
        for k in raw:
            rows.append({
                "date"  : datetime.utcfromtimestamp(k[0] / 1000).strftime("%Y-%m-%d"),
                "open"  : float(k[1]),
                "high"  : float(k[2]),
                "low"   : float(k[3]),
                "close" : float(k[4]),
                "volume": float(k[5]),
            })

        df = pd.DataFrame(rows)
        # 前日比の変動率を計算
        df["change_pct"] = ((df["close"] - df["open"]) / df["open"] * 100).round(2)
        return df

    except Exception as e:
        print(f"[OHLC Fetch Error] {e}")
        return pd.DataFrame()


def fetch_daily_ohlc_jpy(days: int = 30, usd_jpy: float = 155.0) -> pd.DataFrame:
    """
    日足OHLCをJPY換算して返す（USD × usd_jpy レート）。
    usd_jpy は最新の fetch_btc_price() から渡す。
    """
    df = fetch_daily_ohlc(days)
    if df.empty:
        return df
    for col in ["open", "high", "low", "close"]:
        df[f"{col}_jpy"] = (df[col] * usd_jpy).round(0)
    return df


# ─────────────────────────────────────────
# 変動率の計算
# ─────────────────────────────────────────

def calc_change(current: float, past: float) -> float:
    """変動率（%）を計算する。過去値が0の場合は0を返す"""
    if past == 0:
        return 0.0
    return round((current - past) / past * 100, 2)


def get_price_n_hours_ago(history: list[dict], hours: int) -> dict | None:
    """n時間前に最も近いレコードを返す"""
    if not history:
        return None
    target_dt = datetime.now() - timedelta(hours=hours)
    closest = min(
        history,
        key=lambda x: abs(datetime.fromisoformat(x["timestamp"]) - target_dt)
    )
    # 対象時刻との差が2時間以上あれば None（データ不足）
    diff = abs(datetime.fromisoformat(closest["timestamp"]) - target_dt)
    return closest if diff < timedelta(hours=2) else None


def get_price_n_days_ago(history: list[dict], days: int) -> dict | None:
    """n日前に最も近いレコードを返す"""
    return get_price_n_hours_ago(history, days * 24)


def build_report(history: list[dict]) -> dict:
    """
    現在価格と各期間の変動率を計算してレポートを生成する。

    Returns
    -------
    dict
        レポートデータ（現在価格・各期間変動率）
    """
    if not history:
        return {}

    current = history[-1]
    now_usd = current["usd"]
    now_jpy = current["jpy"]
    now_str = datetime.fromisoformat(current["timestamp"]).strftime("%Y/%m/%d %H:%M")

    def change_row(label: str, hours: int) -> dict:
        past = get_price_n_hours_ago(history, hours)
        if not past:
            return {"label": label, "usd_chg": None, "jpy_chg": None}
        return {
            "label"  : label,
            "usd_chg": calc_change(now_usd, past["usd"]),
            "jpy_chg": calc_change(now_jpy, past["jpy"]),
            "past_usd": past["usd"],
            "past_jpy": past["jpy"],
        }

    # ── 月初・下旬の計算 ─────────────────────────────
    now_dt  = datetime.now()
    # 月初（1日の最初のレコード）
    month_start_dt = now_dt.replace(day=1, hour=0, minute=0, second=0)
    month_start_rec = None
    for rec in history:
        if datetime.fromisoformat(rec["timestamp"]) >= month_start_dt:
            month_start_rec = rec
            break

    # 下旬（21日の最初のレコード）
    late_month_dt = now_dt.replace(day=21, hour=0, minute=0, second=0) if now_dt.day >= 21 else None
    late_month_rec = None
    if late_month_dt:
        for rec in history:
            if datetime.fromisoformat(rec["timestamp"]) >= late_month_dt:
                late_month_rec = rec
                break

    # 半月（15日前）
    half_month_rec = get_price_n_days_ago(history, 15)

    periods = [
        change_row("6時間前",  6),
        change_row("12時間前", 12),
        change_row("24時間前", 24),
        change_row("1週間前",  168),
        change_row("2週間前",  336),
    ]

    # 月初比
    if month_start_rec:
        periods.append({
            "label"   : "月初比",
            "usd_chg" : calc_change(now_usd, month_start_rec["usd"]),
            "jpy_chg" : calc_change(now_jpy, month_start_rec["jpy"]),
            "past_usd": month_start_rec["usd"],
            "past_jpy": month_start_rec["jpy"],
        })

    # 半月比
    if half_month_rec:
        periods.append({
            "label"   : "半月比",
            "usd_chg" : calc_change(now_usd, half_month_rec["usd"]),
            "jpy_chg" : calc_change(now_jpy, half_month_rec["jpy"]),
            "past_usd": half_month_rec["usd"],
            "past_jpy": half_month_rec["jpy"],
        })

    # 下旬比
    if late_month_rec:
        periods.append({
            "label"   : "下旬比（21日〜）",
            "usd_chg" : calc_change(now_usd, late_month_rec["usd"]),
            "jpy_chg" : calc_change(now_jpy, late_month_rec["jpy"]),
            "past_usd": late_month_rec["usd"],
            "past_jpy": late_month_rec["jpy"],
        })

    return {
        "now_str" : now_str,
        "now_usd" : now_usd,
        "now_jpy" : now_jpy,
        "usd_jpy" : current.get("usd_jpy", "-"),
        "periods" : periods,
    }


# ─────────────────────────────────────────
# 通知送信
# ─────────────────────────────────────────

def format_report_text(report: dict) -> str:
    """レポートをテキスト形式にフォーマットする（メール・Discord共通）"""
    if not report:
        return "データがありません"

    # 日足 OHLC を取得してレポートに追加
    try:
        df_ohlc_r = fetch_daily_ohlc(days=2)
        if not df_ohlc_r.empty:
            today_r = df_ohlc_r.iloc[-1]
            ohlc_lines = [
                "",
                "【本日の日足】",
                f"  始値 : ${today_r['open']:,.2f}",
                f"  終値 : ${today_r['close']:,.2f}",
                f"  高値 : ${today_r['high']:,.2f}",
                f"  安値 : ${today_r['low']:,.2f}",
                f"  変動 : {today_r['change_pct']:+.2f}%",
            ]
        else:
            ohlc_lines = []
    except Exception:
        ohlc_lines = []

    lines = [
        f"₿ BTC価格レポート【{report['now_str']}】",
        f"{'='*40}",
        f"BTC/USD : ${report['now_usd']:,.2f}",
        f"BTC/JPY : ¥{report['now_jpy']:,.0f}",
        f"USD/JPY : {report['usd_jpy']}",
    ] + ohlc_lines + [
        "",
        "【期間別 変動率】",
    ]

    for p in report["periods"]:
        usd_c = p.get("usd_chg")
        jpy_c = p.get("jpy_chg")
        if usd_c is None:
            lines.append(f"  {p['label']:12s} : データ不足")
        else:
            u_sign = "▲" if usd_c >= 0 else "▼"
            j_sign = "▲" if jpy_c >= 0 else "▼"
            lines.append(
                f"  {p['label']:12s} | USD {u_sign}{abs(usd_c):.2f}%  JPY {j_sign}{abs(jpy_c):.2f}%"
            )

    lines += ["", "📊 BTC Monitor by Python + Streamlit"]
    return "\n".join(lines)


def send_email(settings: dict, subject: str, body: str):
    """Gmailのアプリパスワードを使ってメールを送信する"""
    if not settings["smtp_user"] or not settings["smtp_password"]:
        print("[Email] SMTP設定が不完全です")
        return
    if not settings["emails"]:
        return

    msg = MIMEMultipart()
    msg["From"]    = settings["smtp_user"]
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(settings["smtp_host"], settings["smtp_port"]) as server:
            server.starttls()
            server.login(settings["smtp_user"], settings["smtp_password"])
            for email in settings["emails"]:
                msg["To"] = email
                server.sendmail(settings["smtp_user"], email, msg.as_string())
                print(f"[Email] 送信完了: {email}")
    except Exception as e:
        print(f"[Email Error] {e}")


def send_discord(settings: dict, message: str):
    """Discord Webhook にメッセージを送信する"""
    for webhook_url in settings.get("discord_webhooks", []):
        try:
            payload = {"content": f"```\n{message}\n```"}
            r = requests.post(webhook_url, json=payload, timeout=10)
            r.raise_for_status()
            print(f"[Discord] 送信完了")
        except Exception as e:
            print(f"[Discord Error] {e}")


def notify_all(settings: dict, report: dict):
    """メールとDiscordの両方に通知を送る"""
    text    = format_report_text(report)
    subject = f"₿ BTCレポート {report.get('now_str', '')} | ${report.get('now_usd', '-'):,}"
    send_email(settings, subject, text)
    send_discord(settings, text)


# ─────────────────────────────────────────
# バックグラウンドスケジューラー
# ─────────────────────────────────────────

# Streamlit はリロードのたびにスクリプトを再実行するため、
# session_state でスケジューラーの起動状態を管理する

def job_fetch_price():
    """1時間ごとに実行：価格を取得して履歴に追加"""
    price = fetch_btc_price()
    if price:
        history = load_history()
        history.append(price)
        save_history(history)
        print(f"[Fetch] {price['timestamp']} USD={price['usd']} JPY={price['jpy']}")


def job_notify(notify_counter: list):
    """
    1時間ごとに呼ばれ、6回目（=6時間）ごとに通知を送る。
    notify_counter は [int] の1要素リストで、外から参照できるようにする。
    """
    notify_counter[0] += 1
    if notify_counter[0] >= NOTIFY_INTERVAL:
        notify_counter[0] = 0
        history  = load_history()
        report   = build_report(history)
        settings = load_settings()
        notify_all(settings, report)
        print(f"[Notify] 通知送信完了 {datetime.now()}")


def start_scheduler():
    """APSchedulerでバックグラウンドジョブを開始する"""
    scheduler       = BackgroundScheduler()
    notify_counter  = [0]  # 参照渡しのためリストに包む

    # 1時間ごとに価格取得
    scheduler.add_job(job_fetch_price, "interval", seconds=FETCH_INTERVAL, id="fetch")
    # 1時間ごとにカウントし6時間ごとに通知
    scheduler.add_job(lambda: job_notify(notify_counter), "interval", seconds=FETCH_INTERVAL, id="notify")
    scheduler.start()
    return scheduler


# スケジューラーの二重起動防止
if "scheduler_started" not in st.session_state:
    st.session_state["scheduler_started"] = True
    st.session_state["scheduler"] = start_scheduler()
    # 起動時に1回すぐ価格を取得
    job_fetch_price()


# ─────────────────────────────────────────
# メインUI
# ─────────────────────────────────────────

st.markdown("# ₿ BTC Price Monitor")
st.markdown("1時間ごとに価格を記録し、6時間ごとにメール・Discordへレポートを送信します")
st.divider()

# ── タブ構成 ──────────────────────────────
tab_dashboard, tab_chart, tab_settings = st.tabs(["📊 ダッシュボード", "📈 価格チャート", "⚙️ 通知設定"])


# ══════════════════════════════════════════
# ダッシュボード タブ
# ══════════════════════════════════════════
with tab_dashboard:

    col_refresh, col_status = st.columns([1, 3])
    with col_refresh:
        if st.button("🔄 今すぐ価格を取得", use_container_width=True):
            job_fetch_price()
            st.success("取得完了！")

    history = load_history()
    report  = build_report(history)

    if not report:
        st.info("まだデータがありません。「今すぐ価格を取得」を押してください。")
    else:
        # 現在価格
        st.markdown("### 💰 現在のBTC価格")
        c1, c2, c3 = st.columns(3)
        c1.metric("BTC/USD", f"${report['now_usd']:,.2f}")
        c2.metric("BTC/JPY", f"¥{report['now_jpy']:,.0f}")
        c3.metric("USD/JPY", f"{report['usd_jpy']}")
        st.caption(f"最終更新: {report['now_str']}")

        st.divider()

        # 変動率テーブル
        st.markdown("### 📉📈 期間別 変動率")

        def badge(val):
            if val is None:
                return "－"
            sign = "▲" if val >= 0 else "▼"
            cls  = "badge-up" if val >= 0 else "badge-down"
            return f'<span class="{cls}">{sign}{abs(val):.2f}%</span>'

        rows_html = ""
        for p in report["periods"]:
            usd_c = p.get("usd_chg")
            jpy_c = p.get("jpy_chg")
            rows_html += f"""
            <tr>
              <td style='padding:8px 12px;'>{p['label']}</td>
              <td style='padding:8px 12px; text-align:center;'>{badge(usd_c)}</td>
              <td style='padding:8px 12px; text-align:center;'>{badge(jpy_c)}</td>
            </tr>"""

        st.markdown(f"""
        <div class="card">
        <table style='width:100%; border-collapse:collapse;'>
          <thead>
            <tr style='border-bottom:1px solid rgba(255,255,255,0.15);'>
              <th style='padding:8px 12px; text-align:left; color:#a0b4cc;'>期間</th>
              <th style='padding:8px 12px; text-align:center; color:#a0b4cc;'>BTC/USD 変動率</th>
              <th style='padding:8px 12px; text-align:center; color:#a0b4cc;'>BTC/JPY 変動率</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        </div>
        """, unsafe_allow_html=True)

        st.divider()

        # ── 24時間 始値・終値セクション ────────
        st.markdown("### 🕯 直近の日足 始値・終値（BTC/USDT）")

        # 現在のUSD/JPYレートを履歴から取得
        current_usd_jpy = history[-1].get("usd_jpy", 155.0) if history else 155.0
        df_ohlc = fetch_daily_ohlc_jpy(days=7, usd_jpy=current_usd_jpy)

        if df_ohlc.empty:
            st.warning("OHLCデータの取得に失敗しました")
        else:
            # 直近7日分を新しい順に表示
            df_show = df_ohlc.sort_values("date", ascending=False).head(7).reset_index(drop=True)

            # 今日と昨日のメトリクスを強調表示
            if len(df_show) >= 2:
                today     = df_show.iloc[0]
                yesterday = df_show.iloc[1]

                st.markdown("**📅 本日（最新）と前日の比較**")
                col_t, col_y = st.columns(2)

                with col_t:
                    chg_color = "🟢" if today["change_pct"] >= 0 else "🔴"
                    st.markdown(f"""
                    <div class="card">
                      <b>📅 {today['date']}（本日）</b><br><br>
                      🔓 始値 USD: <b>${today['open']:,.2f}</b>　JPY: <b>¥{today['open_jpy']:,.0f}</b><br>
                      🔒 終値 USD: <b>${today['close']:,.2f}</b>　JPY: <b>¥{today['close_jpy']:,.0f}</b><br>
                      📈 高値 USD: ${today['high']:,.2f}<br>
                      📉 安値 USD: ${today['low']:,.2f}<br>
                      {chg_color} 日中変動: <b>{today['change_pct']:+.2f}%</b>
                    </div>
                    """, unsafe_allow_html=True)

                with col_y:
                    chg_color_y = "🟢" if yesterday["change_pct"] >= 0 else "🔴"
                    st.markdown(f"""
                    <div class="card">
                      <b>📅 {yesterday['date']}（前日）</b><br><br>
                      🔓 始値 USD: <b>${yesterday['open']:,.2f}</b>　JPY: <b>¥{yesterday['open_jpy']:,.0f}</b><br>
                      🔒 終値 USD: <b>${yesterday['close']:,.2f}</b>　JPY: <b>¥{yesterday['close_jpy']:,.0f}</b><br>
                      📈 高値 USD: ${yesterday['high']:,.2f}<br>
                      📉 安値 USD: ${yesterday['low']:,.2f}<br>
                      {chg_color_y} 日中変動: <b>{yesterday['change_pct']:+.2f}%</b>
                    </div>
                    """, unsafe_allow_html=True)

            # 直近7日間テーブル
            st.markdown("**📋 直近7日間 日足テーブル（USD）**")

            def style_change(val):
                """変動率に色をつける"""
                if val is None:
                    return "－"
                sign = "▲" if val >= 0 else "▼"
                cls  = "badge-up" if val >= 0 else "badge-down"
                return f'<span class="{cls}">{sign}{abs(val):.2f}%</span>'

            rows_ohlc = ""
            for _, row in df_show.iterrows():
                rows_ohlc += f"""
                <tr style='border-bottom:1px solid rgba(255,255,255,0.07);'>
                  <td style='padding:7px 10px;'>{row['date']}</td>
                  <td style='padding:7px 10px; text-align:right;'>${row['open']:,.2f}<br><small style='color:#6b8aad;'>¥{row['open_jpy']:,.0f}</small></td>
                  <td style='padding:7px 10px; text-align:right;'>${row['close']:,.2f}<br><small style='color:#6b8aad;'>¥{row['close_jpy']:,.0f}</small></td>
                  <td style='padding:7px 10px; text-align:right; color:#4ade80;'>${row['high']:,.2f}</td>
                  <td style='padding:7px 10px; text-align:right; color:#f87171;'>${row['low']:,.2f}</td>
                  <td style='padding:7px 10px; text-align:center;'>{style_change(row['change_pct'])}</td>
                </tr>"""

            st.markdown(f"""
            <div class="card" style="overflow-x:auto;">
            <table style='width:100%; border-collapse:collapse; font-size:0.9rem;'>
              <thead>
                <tr style='border-bottom:1px solid rgba(255,255,255,0.2);'>
                  <th style='padding:7px 10px; text-align:left; color:#a0b4cc;'>日付</th>
                  <th style='padding:7px 10px; text-align:right; color:#a0b4cc;'>始値</th>
                  <th style='padding:7px 10px; text-align:right; color:#a0b4cc;'>終値</th>
                  <th style='padding:7px 10px; text-align:right; color:#4ade80;'>高値</th>
                  <th style='padding:7px 10px; text-align:right; color:#f87171;'>安値</th>
                  <th style='padding:7px 10px; text-align:center; color:#a0b4cc;'>日中変動</th>
                </tr>
              </thead>
              <tbody>{rows_ohlc}</tbody>
            </table>
            </div>
            """, unsafe_allow_html=True)

        st.divider()

        # 手動通知
        st.markdown("### 📤 今すぐレポートを送信")
        if st.button("メール・Discordにレポートを送信", use_container_width=False):
            settings = load_settings()
            notify_all(settings, report)
            st.success("送信しました！")
        st.caption(f"次の自動送信は6時間ごとに実行されます")


# ══════════════════════════════════════════
# チャート タブ
# ══════════════════════════════════════════
with tab_chart:

    chart_type = st.radio("チャート種別", ["📉 折れ線（時系列）", "🕯 ローソク足（日足）"], horizontal=True)

    # ── ローソク足チャート ──────────────────
    if chart_type == "🕯 ローソク足（日足）":
        candle_days = st.slider("表示日数", min_value=7, max_value=30, value=14)
        history_now = load_history()
        cur_rate    = history_now[-1].get("usd_jpy", 155.0) if history_now else 155.0
        df_ohlc     = fetch_daily_ohlc_jpy(days=candle_days, usd_jpy=cur_rate)

        if df_ohlc.empty:
            st.warning("OHLCデータの取得に失敗しました")
        else:
            currency = st.radio("通貨", ["USD", "JPY"], horizontal=True)
            open_col  = "open"      if currency == "USD" else "open_jpy"
            high_col  = "high"      if currency == "USD" else "high_jpy"
            low_col   = "low"       if currency == "USD" else "low_jpy"
            close_col = "close"     if currency == "USD" else "close_jpy"
            prefix    = "$"         if currency == "USD" else "¥"
            fmt       = ",.2f"      if currency == "USD" else ",.0f"

            fig_c = go.Figure(data=[go.Candlestick(
                x=df_ohlc["date"],
                open =df_ohlc[open_col],
                high =df_ohlc[high_col],
                low  =df_ohlc[low_col],
                close=df_ohlc[close_col],
                increasing=dict(line=dict(color="#4ade80"), fillcolor="#14532d"),
                decreasing=dict(line=dict(color="#f87171"), fillcolor="#4c0519"),
                name=f"BTC/{currency}",
            )])
            fig_c.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(255,255,255,0.03)",
                font=dict(color="#e8f0fe"),
                height=420, margin=dict(t=20, b=30),
                xaxis=dict(gridcolor="rgba(255,255,255,0.07)", rangeslider=dict(visible=False)),
                yaxis=dict(title=f"BTC/{currency}", gridcolor="rgba(255,255,255,0.07)"),
                hovermode="x unified",
            )
            st.markdown(f"#### 🕯 BTC/{currency} 日足ローソク足")
            st.plotly_chart(fig_c, use_container_width=True)

            # 始値・終値サマリーテーブル
            st.markdown(f"#### 📋 日足 始値・終値テーブル（{currency}）")
            df_table = df_ohlc[["date", open_col, close_col, high_col, low_col, "change_pct"]].copy()
            df_table.columns = ["日付", "始値", "終値", "高値", "安値", "日中変動(%)"]
            df_table = df_table.sort_values("日付", ascending=False).reset_index(drop=True)
            st.dataframe(df_table, use_container_width=True, hide_index=True)

    # ── 折れ線チャート ──────────────────────
    else:
        history = load_history()

        if len(history) < 2:
            st.info("チャートを表示するにはデータが2件以上必要です。しばらくお待ちください。")
        else:
            df = pd.DataFrame(history)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

            period_choice = st.radio(
                "表示期間",
                ["24時間", "1週間", "2週間", "全期間"],
                horizontal=True,
            )
            period_hours = {"24時間": 24, "1週間": 168, "2週間": 336, "全期間": 99999}
            cutoff  = datetime.now() - timedelta(hours=period_hours[period_choice])
            df_view = df[df["timestamp"] >= cutoff]

            if df_view.empty:
                st.warning("選択期間のデータがありません")
            else:
                # USD チャート
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_view["timestamp"], y=df_view["usd"],
                    mode="lines", name="BTC/USD",
                    line=dict(color="#f0b429", width=2),
                    hovertemplate="%{x}<br>$%{y:,.2f}<extra></extra>",
                ))
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(255,255,255,0.03)",
                    font=dict(color="#e8f0fe"),
                    height=280, margin=dict(t=10, b=30),
                    xaxis=dict(gridcolor="rgba(255,255,255,0.07)"),
                    yaxis=dict(title="USD", gridcolor="rgba(255,255,255,0.07)"),
                    hovermode="x unified",
                )
                st.markdown("#### BTC/USD")
                st.plotly_chart(fig, use_container_width=True)

                # JPY チャート
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=df_view["timestamp"], y=df_view["jpy"],
                    mode="lines", name="BTC/JPY",
                    line=dict(color="#60a5fa", width=2),
                    hovertemplate="%{x}<br>¥%{y:,.0f}<extra></extra>",
                ))
                fig2.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(255,255,255,0.03)",
                    font=dict(color="#e8f0fe"),
                    height=280, margin=dict(t=10, b=30),
                    xaxis=dict(gridcolor="rgba(255,255,255,0.07)"),
                    yaxis=dict(title="JPY", gridcolor="rgba(255,255,255,0.07)"),
                    hovermode="x unified",
                )
                st.markdown("#### BTC/JPY")
                st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════
# 通知設定 タブ
# ══════════════════════════════════════════
with tab_settings:
    settings = load_settings()

    st.markdown("### 📧 メール設定（Gmail推奨）")
    st.markdown("""
    <div class="card">
      <b>Gmailアプリパスワードの取得方法：</b><br>
      1. Googleアカウント → セキュリティ → 2段階認証をオン<br>
      2. 「アプリパスワード」を検索 → 「メール」で生成<br>
      3. 生成された16文字のパスワードを下に入力
    </div>
    """, unsafe_allow_html=True)

    smtp_user = st.text_input("送信元 Gmail アドレス", value=settings.get("smtp_user", ""), placeholder="your@gmail.com")
    smtp_pass = st.text_input("Gmail アプリパスワード", value=settings.get("smtp_password", ""), type="password", placeholder="xxxx xxxx xxxx xxxx")

    st.markdown("**通知先メールアドレス（複数登録可）**")
    emails_text = st.text_area(
        "1行に1アドレス",
        value="\n".join(settings.get("emails", [])),
        height=100,
        placeholder="user1@example.com\nuser2@example.com",
    )

    st.divider()
    st.markdown("### 🎮 Discord Webhook 設定")
    st.markdown("""
    <div class="card">
      <b>Discord Webhook URLの取得方法：</b><br>
      1. Discordサーバー → チャンネル設定 → 連携サービス<br>
      2. 「ウェブフック」→「新しいウェブフック」→ URLをコピー
    </div>
    """, unsafe_allow_html=True)

    webhooks_text = st.text_area(
        "Webhook URL（1行に1つ・複数登録可）",
        value="\n".join(settings.get("discord_webhooks", [])),
        height=80,
        placeholder="https://discord.com/api/webhooks/...",
    )

    st.divider()

    col_save, col_test = st.columns(2)
    with col_save:
        if st.button("💾 設定を保存", use_container_width=True, type="primary"):
            new_settings = {
                "emails"          : [e.strip() for e in emails_text.splitlines() if e.strip()],
                "discord_webhooks": [w.strip() for w in webhooks_text.splitlines() if w.strip()],
                "smtp_host"       : "smtp.gmail.com",
                "smtp_port"       : 587,
                "smtp_user"       : smtp_user.strip(),
                "smtp_password"   : smtp_pass.strip(),
            }
            save_settings(new_settings)
            st.success("保存しました！")

    with col_test:
        if st.button("📤 テスト通知を送信", use_container_width=True):
            history = load_history()
            report  = build_report(history)
            s = load_settings()
            if not s["emails"] and not s["discord_webhooks"]:
                st.error("通知先が設定されていません")
            elif not report:
                st.error("価格データがありません。先にデータを取得してください")
            else:
                notify_all(s, report)
                st.success("テスト送信しました！")

    st.divider()
    st.markdown("### ℹ️ 動作状況")
    history = load_history()
    st.metric("記録済み価格データ件数", f"{len(history)} 件")
    if history:
        oldest = datetime.fromisoformat(history[0]["timestamp"]).strftime("%Y/%m/%d %H:%M")
        newest = datetime.fromisoformat(history[-1]["timestamp"]).strftime("%Y/%m/%d %H:%M")
        st.caption(f"記録期間: {oldest} 〜 {newest}")
    st.caption("自動取得: 1時間ごと ｜ 自動通知: 6時間ごと")
