# 🏋️ Peak Fitness Analytics ETL Project

**Author:** Ziyi Yan  
**Completed:** August 25, 2025  
**Tech Stack:** AWS Glue, PySpark, S3, Athena, Glue Data Catalog

---

## 📘 Overview

This project builds a cloud-based data pipeline to transform and analyze user behavior from a fictional fitness studio called **Peak Fitness**. The goal is to produce marketing-ready data for **email campaigns** and **in-studio leaderboard displays**.

---

## 🗂️ Source Data

All data is stored in a shared AWS S3 requester-pays bucket:

s3://peak-fitness-data-raw/
├── peak-fitness-historical/non-pii/mindbody_schedule.csv
├── peak-fitness-stream/non-pii/events/.json
├── peak-fitness-stream/non-pii/schedule/.json

yaml
Copy
Edit

---

## 🛠️ Glue ETL Jobs

| Job Name                             | Description                                         |
|--------------------------------------|-----------------------------------------------------|
| `transform_fact_class_signups`      | Parses the CSV of historical class schedules        |
| `fact_event_table`                  | Parses JSON events (signup, cancel, browse)         |
| `email_segment_inactive_users`      | Segments users who have not signed up in 7+ days    |
| `email_segments_export`             | Exports all email segment CSVs to S3               |
| `generate_leaderboard_top_attendees`| Builds weekly user leaderboard                      |

---

## 🎯 Final Use Cases Delivered

### ✅ 1. Marketing Segments for Email Campaigns

Segmentation includes:
- `inactive_members_7days.csv`
- `power_users.csv` (top 10%)
- `churn_risk_users.csv`

Saved to:
s3://ziyiyan-peakfitness/marketing_segments/

yaml
Copy
Edit

---

### ✅ 2. Leaderboard Feature

Weekly leaderboard of top class attendees by location.

Saved to:
s3://ziyiyan-peakfitness/leaderboards/top_attendees_by_location.csv
