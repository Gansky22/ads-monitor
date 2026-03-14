import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

ACCESS_TOKEN = "EAAWxEf5o0ZCkBQ9g1rMepbNsqhloneaDUMib5FnZBe1blGIG8yjtFuvfxCDBC1L7ZBTe0H9qkNOZBXJ0QUM0WAMyBHZCijkriMGLZA3ngFJOWxZBbmdOYDH4B3xIll3BUlZA9TWH5CclIczub5l9UVGWj0KYAnZBVcinZCsxi8Bis8dxVOl2ZCH5SZClSkCqHxcJUPBjE0BhrHgfJyJGS8rbfQkXJPzQTrTwLn24gHGZA"
TELEGRAM_TOKEN = "8682205595:AAH2Xhl4XP8nf_Q-HQNL8iYDATkN70ImMNg"
CHAT_ID = "5795118271"

# 监控间隔秒数
CHECK_INTERVAL_SECONDS = 30

# 历史总表
HISTORY_FILE = "ads_history.xlsx"

# 周报输出文件夹
REPORT_FOLDER = "weekly_reports"

# 广告账户与客户名称
ACCOUNTS = {

"act_812315907289211": "Nison",

"act_1153431505845097": "fusion",

"act_1621507679002803": "Steel",

"act_1333278190720681": "EBS",

"act_1327516009134912": "Dreamztech"

}

# 记录上一次状态，避免重复提醒
last_status = {}

# 记录本周是否已生成过周报
last_report_week = None


def send_telegram(msg: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": msg
    }
    try:
        r = requests.post(url, data=data, timeout=20)
        print("Telegram:", r.status_code, r.text)
    except Exception as e:
        print("Telegram 发送失败:", e)


def get_ads(account_id: str) -> list:
    url = f"https://graph.facebook.com/v19.0/{account_id}/ads"
    params = {
        "fields": "id,name,status,effective_status",
        "access_token": ACCESS_TOKEN
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        data = r.json()

        if "error" in data:
            print(f"[{account_id}] Facebook API 错误:", data["error"])
            return []

        return data.get("data", [])
    except Exception as e:
        print(f"[{account_id}] 获取广告失败:", e)
        return []


def append_history(rows: list) -> None:
    if not rows:
        return

    new_df = pd.DataFrame(rows)

    if os.path.exists(HISTORY_FILE):
        old_df = pd.read_excel(HISTORY_FILE)
        all_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        all_df = new_df

    all_df.to_excel(HISTORY_FILE, index=False)


def check_ads() -> None:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history_rows = []

    for account_id, client_name in ACCOUNTS.items():
        ads = get_ads(account_id)

        for ad in ads:
            ad_id = ad.get("id", "")
            ad_name = ad.get("name", "")
            status = ad.get("status", "")
            effective_status = ad.get("effective_status", "")

            print(
                f"[{client_name}] 广告: {ad_name} | "
                f"status: {status} | effective_status: {effective_status}"
            )

            history_rows.append({
                "datetime": now_str,
                "client_name": client_name,
                "account_id": account_id,
                "ad_id": ad_id,
                "ad_name": ad_name,
                "status": status,
                "effective_status": effective_status
            })

            current_state = f"{status}|{effective_status}"

            # 第一次看到这条广告，只记录，不通知
            if ad_id not in last_status:
                last_status[ad_id] = current_state
                continue

            # 只有状态变化才通知一次
            if last_status[ad_id] != current_state:
                msg = (
                    f"⚠ 广告状态改变\n"
                    f"客户: {client_name}\n"
                    f"账户: {account_id}\n"
                    f"广告: {ad_name}\n"
                    f"旧状态: {last_status[ad_id]}\n"
                    f"新状态: {current_state}"
                )
                send_telegram(msg)
                last_status[ad_id] = current_state

    append_history(history_rows)


def generate_weekly_reports() -> None:
    if not os.path.exists(HISTORY_FILE):
        print("还没有历史数据，暂时不能生成周报。")
        return

    os.makedirs(REPORT_FOLDER, exist_ok=True)

    df = pd.read_excel(HISTORY_FILE)
    if df.empty:
        print("历史表为空。")
        return

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)

    weekly_df = df[(df["datetime"] >= start_time) & (df["datetime"] <= end_time)].copy()

    if weekly_df.empty:
        print("最近 7 天没有数据。")
        return

    for client_name in weekly_df["client_name"].unique():
        client_df = weekly_df[weekly_df["client_name"] == client_name].copy()

        latest_df = (
            client_df.sort_values("datetime")
            .groupby(["account_id", "ad_id", "ad_name"], as_index=False)
            .tail(1)
        )

        summary_df = (
            client_df.groupby(["effective_status"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )

        pivot_df = (
            client_df.groupby(["datetime", "account_id", "ad_name", "status", "effective_status"])
            .size()
            .reset_index(name="rows")
        )

        filename = os.path.join(
            REPORT_FOLDER,
            f"{client_name}_weekly_report_{end_time.strftime('%Y%m%d')}.xlsx"
        )

        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            latest_df.to_excel(writer, sheet_name="latest_status", index=False)
            client_df.to_excel(writer, sheet_name="history_7days", index=False)
            summary_df.to_excel(writer, sheet_name="summary", index=False)
            pivot_df.to_excel(writer, sheet_name="detail_view", index=False)

        print("已生成周报:", filename)


def should_generate_report() -> bool:
    global last_report_week

    now = datetime.now()
    current_week = f"{now.isocalendar().year}-W{now.isocalendar().week}"

    # 每周一早上 9 点后生成一次
    if now.weekday() == 0 and now.hour >= 9:
        if last_report_week != current_week:
            last_report_week = current_week
            return True

    return False


while True:
    try:
        check_ads()

        if should_generate_report():
            generate_weekly_reports()
            send_telegram("📊 本周客户广告 Excel 周报已生成。")

    except Exception as e:
        print("系统错误:", e)

    time.sleep(CHECK_INTERVAL_SECONDS)