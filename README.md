<p align="center">
  <img src="https://img.shields.io/badge/AeonCoreX-Official%20Project-0A0A0A?style=for-the-badge&logo=vercel&logoColor=white"/>
  <img src="https://img.shields.io/badge/StreamX%20Ultra-Live%20TV%20Platform-1E90FF?style=for-the-badge&logo=tvtime&logoColor=white"/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/IPTV-JSON%20Data-orange?style=flat-square"/>
  <img src="https://img.shields.io/badge/Auto%20Updated-Daily-brightgreen?style=flat-square"/>
  <img src="https://img.shields.io/badge/Status-Production-success?style=flat-square"/>
  <img src="https://img.shields.io/badge/HD%20Streams-Supported-blue?style=flat-square"/>
</p>

<p align="center">
  <img src="https://img.shields.io/github/stars/cybernahid-dev/streamx-iptv-data?style=social"/>
  <img src="https://img.shields.io/github/forks/cybernahid-dev/streamx-iptv-data?style=social"/>
  <img src="https://img.shields.io/github/license/cybernahid-dev/streamx-iptv-data"/>
</p>

---

# 🚀 StreamX Ultra – Official IPTV Data Repository

### 🌐 AeonCoreX Official Live TV Infrastructure

**StreamX Ultra** is the official next-generation **Live TV & IPTV platform** by **AeonCoreX**.  
This repository contains the **core IPTV data backbone** that powers StreamX Ultra across all platforms.

> ⚠️ This is an **official AeonCoreX production repository**.

---

## 🏢 About AeonCoreX

**AeonCoreX** is a future-driven technology company building scalable platforms in:

- Live TV & IPTV Infrastructure  
- Streaming & Media Automation  
- Cyber & Cloud Systems  
- Data-Driven Platforms  

**StreamX Ultra** is AeonCoreX’s **official live TV platform**, and this repository serves as its **single source of truth for IPTV data**.

---

## 📌 Purpose of This Repository

This repository provides **structured, category-wise JSON data** consumed directly by the **StreamX Ultra application**.

### Key Responsibilities:
- Centralized IPTV data source  
- Automatic channel updates  
- Category-based channel organization  
- Live sports & upcoming event metadata  
- Fast, scalable, and app-friendly structure  

---

## 🧠 High-Level Architecture

Public M3U Sources ↓ Automation Engine (Python) ↓ Validated & Normalized JSON ↓ GitHub Repository (This Repo) ↓ StreamX Ultra App ↓ End Users (Live TV Experience)

---

## 📂 Repository Structure

streamx-iptv-data/ │ ├── index.json │   └── Master entry point for StreamX Ultra │ ├── categories/ │   ├── bangladesh.json │   ├── india.json │   ├── usa.json │   ├── sports.json │   ├── movies.json │   ├── kids.json │   └── informative.json │ ├── assets/ │   └── Logos, icons & branding resources │ ├── README.md ├── LICENSE └── .gitignore

---

## 🗂️ Available Categories

### 🌍 Regional
- Bangladesh 🇧🇩
- India 🇮🇳
- USA 🇺🇸
- (Expandable worldwide)

### 🏅 Sports
- Live sports channels  
- Event-based streams  
- Upcoming match metadata (time & status)

### 🎬 Movies & Entertainment
- Action & Entertainment channels  
- Movie-focused IPTV streams  

### 🧒 Kids
- Cartoon & kids TV  
- Educational content  
- Parental-safe categorization  

### 🧠 Informative
- Discovery & Science  
- Documentary  
- Nature & Wildlife  
- History & Civilization  
- Technology & Space  

---

## ⚡ Core Features

- 🔄 **Automated Daily Updates**
- 📦 **Category-wise JSON Architecture**
- 📺 **HD Stream Metadata**
- ⭐ **Featured Channel System**
- ⏰ **Upcoming Sports Event Support**
- 🚀 **Optimized for Fast App Load**
- 🔐 **Production-Safe & Scalable**

---

## 🔄 Automation System

This repository is maintained using an **internal automation pipeline**:

- M3U sources are fetched automatically  
- Channels are parsed & validated  
- JSON files are regenerated  
- Data is pushed to GitHub  
- StreamX Ultra app syncs instantly  

⏱ Update frequency: **Daily (or configurable)**

> Automation scripts are intentionally excluded from GitHub for security reasons.

---

## 🔗 App Integration

The StreamX Ultra app only needs **one endpoint**:


index.json

From this file, the app dynamically loads:
- All categories  
- All channels  
- All metadata  

No hard-coded channels.  
No app updates required for content changes.

---

## ⚠️ Usage & Distribution Policy

- Intended for **StreamX Ultra** and **AeonCoreX-approved platforms**  
- Redistribution without permission is discouraged  
- Private / paid streams are not included  

---

## 📄 License & Copyright

© 2025 AeonCoreX

This repository is licensed under the **MIT License**.  
However, **AeonCoreX**, **StreamX Ultra**, branding, and platform identity remain the **exclusive property of AeonCoreX**.

See [LICENSE](LICENSE) for details.

---

## ⚖️ Legal Disclaimer & Attribution

### 1. Attribution
This project utilizes data, metadata, and artwork (including logos, posters, and icons) provided by **[TheTVDB](https://thetvdb.com/)**. This project is not officially endorsed, certified, or sponsored by TheTVDB or its affiliates. All API usage strictly adheres to TheTVDB's Terms of Service.

### 2. Fair Use Notice
This repository is maintained strictly for **educational, research, and non-commercial development purposes**. 

Any copyrighted material, including but not limited to media logos, network trademarks, and promotional imagery, registered by their respective production houses (e.g., Netflix, Disney, HBO, Warner Bros.) or networks, is used here under the **Fair Use** doctrine (Section 107 of the U.S. Copyright Act). This usage is intended solely for informational indexing and user-interface simulation, without any intent to infringe upon the original creators' commercial rights.

### 3. DMCA & Takedown Policy
The owner of this repository does not claim ownership over any retrieved media assets. If you are a copyright owner or an agent thereof and believe that any content hosted in this repository infringes upon your copyrights, you may request a removal. 

Please open a formal GitHub Issue or contact the maintainer directly with valid proof of ownership, and the contested material will be removed promptly within 24-48 hours.


## 🔮 Roadmap

- 🌐 Global country expansion  
- 🧠 AI-based channel recommendations  
- 🔔 Live sports notifications  
- 📊 Popularity & analytics engine  
- 📡 Adaptive bitrate & 4K streams  

---

## Credits
- **IPTV Stream Links:** Public and Free-to-Air (FTA) streams are sourced from the open-source community initiative [iptv-org](https://github.com/iptv-org/iptv).
- **Channel Logos & Artwork:** Logo images and metadata are provided via official API integration by [TheTVDB](https://thetvdb.com/).
- This product uses these public data sources for indexing and user-interface simulation but is not officially endorsed, certified, or sponsored by iptv-org or TheTVDB.


## 🤝 Maintained By

**AeonCoreX – Core Platform Team**  
Project: **StreamX Ultra**  
Role: **Official IPTV Data Infrastructure**

> Built for scale. Designed for the future.
