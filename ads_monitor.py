import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

# =========================
# 基本设置
# =========================
ACCESS_TOKEN = "EAAWxEf5o0ZCkBQ6OIJSv7dKlNdRBHe2R8sl5dsugErxrRTl4HkbN8w1PfdXdNbg0e3gZCBkelxM9fhsLXC7s50r5Sa9U9J2QatOiNlWv5hsiMZCtkDL1gtwXWEegl1tDpCK87U9hQFvbWjJrtENsoSQwb4epz0xefRuSFaYFgn4zMZBFKvwH3jAobOq7"
TELEGRAM_TOKEN = "8682205595:AAH2Xhl4XP8nf_Q-HQNL8iYDATkN70ImMNg"
CHAT_ID = "5795118271"

CHECK_INTERVAL_SECONDS = 30
HISTORY_FILE = "ads_history.xlsx"
REPORT_FOLDER = "weekly_reports"

# 你现在先实验这5个客户
ACCOUNTS = {
    "act_812315907289211": "Nison",
    "act_1153431505845097": "fusion",
    "act_1621507679002803": "Steel",
    "act_1333278190720681": "EBS",
    "act_1327516009134912": "Dreamztech"
}

# 只提醒这些异常状态
ALERT_EFFECTIVE_STATUSES = {
    "PAUSED",
    "CAMPAIGN_PAUSED",
    "ADSET_PAUSED",
    "WITH_ISSUES",
    "DISAPPROVED",
    "PENDING_REVIEW",
    "PREAPPROVED",
    "PENDING_BILLING_INFO",
    "ACCOUNT_DISABLED",
    "ARCHIVED"
}

# 记录广告上一次状态，避免重复提醒
last_status = {}

if last_status.get(ad_id) != effective_status:
    last_status[ad_id] = effective_status

    if effective_status != "ACTIVE":
        send_telegram(...)

# 记录这周是否已生成过周报
last_report_week = None


# =========================
# Telegram 发送文字
# =========================
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


# =========================
# Telegram 发送文件
# =========================
def send_telegram_file(file_path: str, caption: str = "") -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"

    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            data = {
                "chat_id": CHAT_ID,
                "caption": caption
            }
            r = requests.post(url, data=data, files=files, timeout=60)
            print("Telegram file:", r.status_code, r.text)
    except Exception as e:
        print("Telegram 文件发送失败:", e)


# =========================
# 安全请求 Facebook API
# =========================
def safe_request(url: str, params: dict) -> dict:
    try:
        r = requests.get(url, params=params, timeout=30)
        data = r.json()

        if "error" in data:
            error_info = data["error"]
            print("Facebook API 错误:", error_info)

            if error_info.get("code") == 190:
                send_telegram("⚠ Facebook Access Token 已过期，请立即更新。")

            return {}

        return data
    except Exception as e:
        print("请求失败:", e)
        return {}


# =========================
# 获取广告状态
# =========================
def get_ads(account_id: str) -> list:
    url = f"https://graph.facebook.com/v19.0/{account_id}/ads"
    params = {
        "fields": "id,name,status,effective_status",
        "access_token": ACCESS_TOKEN
    }
    data = safe_request(url, params)
    return data.get("data", [])


# =========================
# 获取一周广告表现数据
# =========================
def get_account_insights(account_id: str, since_date: str, until_date: str) -> list:
    url = f"https://graph.facebook.com/v19.0/{account_id}/insights"
    params = {
        "level": "ad",
        "time_range": f'{{"since":"{since_date}","until":"{until_date}"}}',
        "fields": ",".join([
            "campaign_name",
            "adset_name",
            "ad_name",
            "spend",
            "impressions",
            "clicks",
            "ctr",
            "cpc",
            "actions",
            "cost_per_action_type"
        ]),
        "limit": 500,
        "access_token": ACCESS_TOKEN
    }

    all_rows = []

    while True:
        data = safe_request(url, params)
        rows = data.get("data", [])
        all_rows.extend(rows)

        paging = data.get("paging", {})
        next_url = paging.get("next")

        if not next_url:
            break

        url = next_url
        params = {}

    return all_rows


# =========================
# 从 actions 取 leads
# =========================
def extract_leads(actions: list) -> int:
    if not isinstance(actions, list):
        return 0

    total = 0
    for item in actions:
        if item.get("action_type") in [
            "lead",
            "onsite_conversion.lead_grouped",
            "offsite_conversion.fb_pixel_lead"
        ]:
            try:
                total += int(float(item.get("value", 0)))
            except Exception:
                pass

    return total


# =========================
# 从 cost_per_action_type 取 CPL
# =========================
def extract_cost_per_lead(cost_per_action_type: list) -> float:
    if not isinstance(cost_per_action_type, list):
        return 0.0

    for item in cost_per_action_type:
        if item.get("action_type") in [
            "lead",
            "onsite_conversion.lead_grouped",
            "offsite_conversion.fb_pixel_lead"
        ]:
            try:
                return float(item.get("value", 0))
            except Exception:
                return 0.0

    return 0.0


# =========================
# 写入历史表
# =========================
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


# =========================
# 监控广告状态
# =========================
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

            # 第一次记录，不提醒
            if ad_id not in last_status:
                last_status[ad_id] = current_state
                continue

            # 状态有变化才处理
            if last_status[ad_id] != current_state:
                old_state = last_status[ad_id]
                last_status[ad_id] = current_state

                # 只有异常状态才提醒
                if effective_status in ALERT_EFFECTIVE_STATUSES:
                    msg = (
                        f"⚠ 广告异常状态改变\n"
                        f"客户: {client_name}\n"
                        f"账户: {account_id}\n"
                        f"广告: {ad_name}\n"
                        f"旧状态: {old_state}\n"
                        f"新状态: {current_state}"
                    )
                    send_telegram(msg)

    append_history(history_rows)


# =========================
# 每周生成报告并发到 Telegram
# =========================
def generate_weekly_reports() -> None:
    os.makedirs(REPORT_FOLDER, exist_ok=True)

    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)

    since_date = start_time.strftime("%Y-%m-%d")
    until_date = end_time.strftime("%Y-%m-%d")

    summary_messages = []

    for account_id, client_name in ACCOUNTS.items():
        insights = get_account_insights(account_id, since_date, until_date)

        if not insights:
            print(f"{client_name} 最近7天没有 insights 数据")
            continue

        report_rows = []
        total_spend = 0.0
        total_leads = 0
        total_clicks = 0
        total_impressions = 0

        for row in insights:
            spend = float(row.get("spend", 0) or 0)
            impressions = int(float(row.get("impressions", 0) or 0))
            clicks = int(float(row.get("clicks", 0) or 0))
            ctr = float(row.get("ctr", 0) or 0)
            cpc = float(row.get("cpc", 0) or 0)
            leads = extract_leads(row.get("actions", []))
            cpl = extract_cost_per_lead(row.get("cost_per_action_type", []))

            total_spend += spend
            total_leads += leads
            total_clicks += clicks
            total_impressions += impressions

            report_rows.append({
                "Client Name": client_name,
                "Account ID": account_id,
                "Campaign Name": row.get("campaign_name", ""),
                "Ad Set Name": row.get("adset_name", ""),
                "Ad Name": row.get("ad_name", ""),
                "Amount Spent": spend,
                "Impressions": impressions,
                "Clicks": clicks,
                "CTR (%)": ctr,
                "CPC": cpc,
                "Total Leads": leads,
                "Cost Per Lead": cpl
            })

        df = pd.DataFrame(report_rows)

        overall_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        overall_cpl = (total_spend / total_leads) if total_leads > 0 else 0

        valid_cpl_df = df[df["Total Leads"] > 0].copy()
        best_ad = None
        worst_ad = None

        if not valid_cpl_df.empty:
            best_ad = valid_cpl_df.sort_values("Cost Per Lead", ascending=True).iloc[0]
            worst_ad = valid_cpl_df.sort_values("Cost Per Lead", ascending=False).iloc[0]

        summary_df = pd.DataFrame([{
            "Client Name": client_name,
            "Account ID": account_id,
            "Report Period": f"{since_date} to {until_date}",
            "Total Spend": round(total_spend, 2),
            "Total Leads": total_leads,
            "Total Clicks": total_clicks,
            "Total Impressions": total_impressions,
            "Overall CTR (%)": round(overall_ctr, 2),
            "Overall CPL": round(overall_cpl, 2)
        }])

        filename = os.path.join(
            REPORT_FOLDER,
            f"{client_name}_weekly_report_{end_time.strftime('%Y%m%d')}.xlsx"
        )

        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            df.to_excel(writer, sheet_name="Ads Data", index=False)

            if best_ad is not None and worst_ad is not None:
                best_worst_df = pd.DataFrame([
                    {
                        "Type": "Best Ad",
                        "Ad Name": best_ad["Ad Name"],
                        "Campaign Name": best_ad["Campaign Name"],
                        "Amount Spent": best_ad["Amount Spent"],
                        "Total Leads": best_ad["Total Leads"],
                        "Cost Per Lead": best_ad["Cost Per Lead"]
                    },
                    {
                        "Type": "Worst Ad",
                        "Ad Name": worst_ad["Ad Name"],
                        "Campaign Name": worst_ad["Campaign Name"],
                        "Amount Spent": worst_ad["Amount Spent"],
                        "Total Leads": worst_ad["Total Leads"],
                        "Cost Per Lead": worst_ad["Cost Per Lead"]
                    }
                ])
                best_worst_df.to_excel(writer, sheet_name="Best_Worst", index=False)

        print("已生成周报:", filename)

        send_telegram_file(
            filename,
            f"📎 {client_name} 每周广告报告\n周期: {since_date} 至 {until_date}"
        )

        summary_messages.append(
            f"{client_name}\n"
            f"Spend: {round(total_spend, 2)}\n"
            f"Leads: {total_leads}\n"
            f"CPL: {round(overall_cpl, 2)}\n"
            f"CTR: {round(overall_ctr, 2)}%"
        )

    if summary_messages:
        final_msg = "📊 Weekly Ads Report 已生成\n\n" + "\n\n".join(summary_messages[:5])
        send_telegram(final_msg)


# =========================
# 是否到每周生成报告时间
# =========================
def should_generate_report() -> bool:
    global last_report_week

    now = datetime.now()
    current_week = f"{now.isocalendar().year}-W{now.isocalendar().week}"

    # 每周一 9:00 后只执行一次
    if now.weekday() == 0 and now.hour >= 9:
        if last_report_week != current_week:
            last_report_week = current_week
            return True

    return False


# =========================
# 主程序
# =========================
if __name__ == "__main__":
    send_telegram("✅ Ads Monitor 已启动")

CHECK_INTERVAL_SECONDS = 3600

    while True:
        try:
            check_ads()

            if should_generate_report():
                generate_weekly_reports()

        except Exception as e:
            print("系统错误:", e)
            send_telegram(f"❌ Ads Monitor 错误: {e}")
            
        time.sleep(CHECK_INTERVAL_SECONDS)