<p align="center">
  <a href="https://github.com/AeonCoreX-Lab">
    <img src="https://raw.githubusercontent.com/AeonCoreX-Lab/.github/main/badges/aeoncorex-badge.svg" alt="AeonCoreX Official Project" width="220">
  </a> 
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

This repository is powered and maintained by **AeonCoreX's Official Automation Engine**. The included Python scraper scripts handle the complete lifecycle of our IPTV data infrastructure.

### 🛡️ Safety & Integrity Standards:
- **100% Safe & Clean:** The automation scripts contain zero malicious code, backdoors, or telemetry. They are fully optimized for lightweight, standard web requests.
- **Strict Compliance:** The core scraper interacts only with public, open-source endpoints (like `iptv-org`) and official APIs (like `TheTVDB`). It strictly respects target rate-limits (`robots.txt`) and does not perform aggressive scraping or DDoS-like behaviors.
- **Enterprise Automation:** Developed internally by the **AeonCoreX Platform Team** using secure environment variables (`GITHUB_TOKEN` / Secrets) to handle data commits safely within GitHub Actions.

⏱ Update frequency: **Daily (Automated via GitHub Actions)**


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

© 2026 AeonCoreX

This repository is licensed under the **MIT License**.  
However, **AeonCoreX**, **StreamX Ultra**, branding, and platform identity remain the **exclusive property of AeonCoreX**.

See [LICENSE](LICENSE) for details.

---

---

## ⚖️ Legal Disclaimer, Attribution & Compliance

### 1. Project Purpose & Scope

**StreamX Ultra** and this repository are maintained strictly for:

- ✅ **Educational** purposes — learning IPTV data structures and API integration
- ✅ **Research** — studying global channel metadata and streaming infrastructure
- ✅ **Non-commercial development** — building open-source IPTV applications
- ✅ **Personal use** — individual developers testing their own apps

**This is NOT a commercial streaming service. We do NOT host, distribute, or retransmit any video content, streams, or broadcasts.**

---

### 2. Third-Party API & Data Attribution

This project relies on official APIs and community-driven open data. Each source operates under its own terms:

| Source | Type | Usage in This Project | Compliance Status |
|--------|------|----------------------|-------------------|
| **[Logo.dev](https://logo.dev)** | Official Logo API | Primary logo source via `img.logo.dev` | ✅ Free tier (500K req/month). Attribution link required in production per [Terms](https://www.logo.dev/legal/terms). |
| **[TheTVDB](https://thetvdb.com)** | Official Media DB | Fallback artwork & metadata | ✅ API key required. Subject to [TVDB TOS](https://thetvdb.com/tos). Not endorsed by TVDB. |
| **[iptv-org](https://github.com/iptv-org)** | Open-Source Community | Channel metadata, IDs, public stream URLs | ✅ Open-source project. Data sourced from `github.io/api/` endpoints. |
| **[tv-logo/tv-logos](https://github.com/tv-logo/tv-logos)** | GitHub Open Repo | Fallback PNG logos | ✅ GitHub CDN raw URLs. Community fair-use for EPG/IPTV apps. |
| **[MarhyCZ Picons](https://github.com/MarhyCZ/picons)** | GitHub Open Repo | Fallback vector logos | ✅ GitHub Pages CDN. Open-source picon collection. |
| **[LyngSat](https://www.lyngsat-logo.com)** | Satellite Info Site | Fallback satellite logos | ✅ Public website. Used via standard HTTP requests. |
| **[Wikipedia](https://en.wikipedia.org)** | Wikimedia Project | Fallback infobox images | ✅ [Wikimedia API Terms](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use). CC BY-SA where applicable. |

**Important:** This project is **not officially endorsed, certified, sponsored, or affiliated** with any of the above entities. All trademarks, service marks, and logos belong to their respective owners.

---

### 3. Intellectual Property & Copyright Notice

#### 3.1 Channel Logos & Trademarks
All channel logos, network trademarks, brand names, and promotional imagery displayed or referenced in this repository are the **exclusive property of their respective owners**, including but not limited to:

- **Broadcasting Networks:** BBC, CNN, ESPN, Fox, NBC, CBS, ABC, Al Jazeera, Sony, Zee, Star, etc.
- **Production Houses:** Netflix, Disney, HBO, Warner Bros., Paramount, Universal, etc.
- **Sports Leagues:** NFL, NBA, FIFA, UEFA, ICC, IPL, etc.
- **Regional Broadcasters:** Every country's respective TV networks

**We do NOT claim ownership, licensing rights, or distribution rights over any of these materials.**

#### 3.2 Fair Use Doctrine (U.S. Copyright Act Section 107)
The use of copyrighted logos and imagery in this repository constitutes **"Fair Use"** as defined under Section 107 of the U.S. Copyright Act. The following factors support this classification:

| Factor | Application |
|--------|-------------|
| **Purpose** | Non-commercial, educational, and transformative use for data indexing |
| **Nature** | Factual metadata and small-resolution logos (not full creative works) |
| **Amount** | Only logo thumbnails used, not full broadcasts or content |
| **Market Effect** | No substitution for original content or commercial licensing |

#### 3.3 No Commercial Exploitation
This repository and its data are **never sold, licensed, or monetized**. There is no:
- Subscription fees
- Pay-per-view charges
- Advertising revenue from logo display
- Resale of aggregated data

---

### 4. Stream Content Disclaimer

#### 4.1 No Video Hosting
**We do NOT host, store, cache, or transmit any video streams.** This repository contains **only metadata**:

- Channel names and IDs
- Logo image URLs (not the actual image files in most cases)
- Category classifications
- Stream URL pointers (publicly available M3U links from iptv-org)

#### 4.2 Third-Party Stream Sources
Any stream URLs referenced in this data originate from:
- **iptv-org public database** — community-maintained, publicly accessible
- **Official broadcaster websites** — freely available streams
- **Open-source IPTV projects** — licensed under their respective terms

**We are NOT responsible for the content, quality, legality, or availability of any third-party streams.**

#### 4.3 User Responsibility
End users and developers utilizing this data are solely responsible for:
- Ensuring compliance with their local laws and regulations
- Verifying they have proper rights to access referenced streams
- Respecting broadcaster terms of service
- Using VPN or geo-unblocking tools in accordance with local laws

---

### 5. Data Accuracy & Availability

#### 5.1 No Guarantees
While we strive for accuracy through automated validation, we provide **no warranties**:

| Aspect | Disclaimer |
|--------|------------|
| **Accuracy** | Channel metadata may contain errors or become outdated |
| **Availability** | Logos and streams may change, move, or become unavailable without notice |
| **Completeness** | Not all world channels are covered; gaps exist |
| **Timeliness** | Updates occur every 6 hours; real-time changes may not reflect immediately |

#### 5.2 Automated Nature
This repository is **100% machine-generated** via GitHub Actions:
- No human editorial oversight on individual channels
- No manual verification of every logo or stream URL
- Algorithmic matching may occasionally produce incorrect associations

---

### 6. DMCA Takedown & Content Removal Policy

#### 6.1 Commitment to Compliance
We respect intellectual property rights and comply with the **Digital Millennium Copyright Act (DMCA)**. If you believe your copyrighted material is improperly used:

#### 6.2 Takedown Request Process

**Step 1: Identify the Content**
- Specific channel ID or logo file name
- URL or file path in this repository
- Your ownership documentation

**Step 2: Submit Request**
| Method | Details |
|--------|---------|
| **GitHub Issue** | Open a new issue with title `[DMCA] Content Removal Request` |
| **Email** | aeoncorexbd@gmail.com |
| **Required Info** | Full legal name, contact info, specific URLs, statement of good faith |

**Step 3: Verification & Action**
- ⏱️ **Acknowledgment:** Within 24 hours
- 🔍 **Review:** 24-48 hours for ownership verification
- 🗑️ **Removal:** Within 48 hours of verified valid request
- 📧 **Notification:** Confirmation sent to requester

#### 6.3 Counter-Notification
If you believe content was removed in error, you may submit a counter-notification with:
- Your contact information
- Identification of removed content
- Statement under penalty of perjury
- Consent to jurisdiction

---

### 7. Privacy & Data Protection

#### 7.1 No User Data Collection
This repository and its automation scripts:
- ❌ Do NOT collect, store, or process any personal user data
- ❌ Do NOT use cookies, trackers, or analytics on end users
- ❌ Do NOT require user registration or authentication
- ✅ Only process publicly available channel metadata

#### 7.2 GitHub Actions Data
Our CI/CD pipeline processes data entirely within GitHub's infrastructure:
- API keys stored in **encrypted GitHub Secrets**
- No secrets exposed in logs or code
- Cache data is ephemeral and non-personal

---

### 8. Jurisdiction & Governing Law

#### 8.1 Applicable Law
This project is governed by the laws of **Bangladesh** (AeonCoreX's registered jurisdiction) and respects:

- U.S. Copyright Act (Fair Use, Section 107)
- DMCA provisions
- EU GDPR (for any incidental data processing)
- Local laws of users' respective countries

#### 8.2 Dispute Resolution
Any disputes arising from this project shall be resolved through:
1. Good-faith negotiation
2. Mediation
3. Arbitration in Dhaka, Bangladesh
4. Final resort: Competent courts in Bangladesh

---

### 9. Changes to This Disclaimer

We reserve the right to update this legal disclaimer at any time. Changes will be:

- Posted in this README with updated timestamp
- Committed with clear message `[LEGAL] Updated disclaimer`
- Not retroactively applied (only affects future usage)

**Last Updated:** 2026-05-20

---

### 10. Contact & Inquiries

| Purpose | Contact |
|---------|---------|
| **General Questions** | Open a GitHub Discussion |
| **Bug Reports** | Open a GitHub Issue |
| **DMCA / Legal** | aeoncorexbd@gmail.com |
| **Business / Licensing** | aeoncorexbd@gmail.com |
| **Security Issues** | aeoncorexbd@gmail.com |

**Response Time:** 24-48 hours for standard inquiries; 24 hours for legal/security issues.

---

### 11. Acknowledgment

By using, forking, or referencing this repository, you acknowledge that you:

1. ✅ Have read and understood this entire disclaimer
2. ✅ Agree to use this data for lawful purposes only
3. ✅ Accept that AeonCoreX bears no liability for third-party content
4. ✅ Will comply with all applicable laws in your jurisdiction
5. ✅ Understand this is an educational/research project, not a streaming service

**If you do NOT agree with these terms, please do NOT use this repository.**

---


## 🔮 Roadmap

- 🌐 Global country expansion  
- 🧠 AI-based channel recommendations  
- 🔔 Live sports notifications  
- 📊 Popularity & analytics engine  
- 📡 Adaptive bitrate & 4K streams  

---

## Credits & Attribution

This repository uses data and media from the following sources:

### Channel Logos

**Primary Logo Source**
- **[Logo.dev](https://logo.dev)** — Official Brand Logo API
  - Free tier: **500,000 API requests/month**
  - Find logos by domain, stock ticker, or brand name
  - Always-up-to-date CDN
  - Attribution required in production

**Fallback Source**

- **[TheTVDB (TVDB)](https://thetvdb.com)** — Company/Channel logo search API
  - Used as the Fallback source for high-quality channel artwork
  - Requires free API key for access
  - All TVDB data is subject to their [Terms of Service](https://thetvdb.com/tos)


- **[tv-logo/tv-logos](https://github.com/tv-logo/tv-logos)** — Open-source TV logo collection
  - Curated PNG logos for 50+ countries
  - Used under community fair-use for IPTV/EPG applications
  - Direct raw URLs served from GitHub CDN

- **[iptv-org/logos.json](https://iptv-org.github.io/api/logos.json)** — Direct ID match

- **[LyngSat](https://www.lyngsat-logo.com)** — Satellite logo database

- **[Wikipedia API](https://en.wikipedia.org)** — Infobox images

- **[MarhyCZ Picons](https://github.com/MarhyCZ/picons)** — GitHub Pages CDN

### Channel Metadata & Stream Links

- **[iptv-org](https://github.com/iptv-org)** — Global IPTV database
  - `channels.json` — Channel metadata (name, country, category, ID mapping)
  - `streams.json` / M3U playlists — Public stream URLs for testing/development
  - API endpoints: `https://iptv-org.github.io/api/`

---

**Disclaimer:**  
This project is intended for **educational and IPTV app development purposes only**.  
All channel logos, names, and stream links are property of their respective broadcasters/networks.  
We do not host any video streams or claim ownership of any media content.



## 🤝 Maintained By

**AeonCoreX – Core Platform Team**  
Project: **StreamX Ultra**  
Role: **Official IPTV Data Infrastructure**

> Built for scale. Designed for the future.
