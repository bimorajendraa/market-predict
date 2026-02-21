"""
News Sentiment module for Finance Analytics.
Analyzes news articles using FinBERT (English) or keyword dictionary (Indonesian).
Includes event tagging via keyword/rule matching.
Includes relevance engine with blacklist, hard keyword filter, and relevance scoring.
"""

import logging
import math
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

from ..db import get_db_cursor

logger = logging.getLogger(__name__)

# ============================================
# Blacklist patterns for generic market research spam
# ============================================
BLACKLIST_PATTERNS: list[str] = [
    "market by platform", "global market", "cagr", "forecast period",
    "research report", "grand view", "mordor intelligence",
    "market size", "market share", "market growth", "compound annual",
    "market trends", "market analysis", "market research",
    "industry analysis", "industry report", "market outlook",
    "market overview", "market dynamics", "market segmentation",
    "key players", "competitive landscape", "market forecast",
    "marketsandmarkets", "alliedmarketresearch", "researchandmarkets",
    "transparency market", "fortune business insights", "exactitude consultancy",
    "polaris market", "imarc group", "data bridge",
]

# ============================================
# Event Impact Model — event → expected horizon + impact
# ============================================
EVENT_IMPACT_MODEL: dict[str, dict] = {
    "earnings_beat":   {"horizon": "1d",  "expected_impact": +0.03, "confidence": 0.7},
    "earnings_miss":   {"horizon": "1d",  "expected_impact": -0.04, "confidence": 0.7},
    "acquisition":     {"horizon": "30d", "expected_impact": +0.05, "confidence": 0.5},
    "dividend":        {"horizon": "7d",  "expected_impact": +0.01, "confidence": 0.5},
    "lawsuit":         {"horizon": "30d", "expected_impact": -0.05, "confidence": 0.4},
    "restructuring":   {"horizon": "30d", "expected_impact": -0.02, "confidence": 0.4},
    "ipo":             {"horizon": "7d",  "expected_impact": +0.02, "confidence": 0.3},
    "partnership":     {"horizon": "7d",  "expected_impact": +0.02, "confidence": 0.5},
    "regulation":      {"horizon": "30d", "expected_impact": -0.03, "confidence": 0.3},
    "rups":            {"horizon": "7d",  "expected_impact": 0.00,  "confidence": 0.3},
    "right_issue":     {"horizon": "7d",  "expected_impact": -0.02, "confidence": 0.5},
    "stock_split":     {"horizon": "1d",  "expected_impact": 0.00,  "confidence": 0.3},
    "buyback":         {"horizon": "7d",  "expected_impact": +0.02, "confidence": 0.5},
    "guidance_up":     {"horizon": "7d",  "expected_impact": +0.02, "confidence": 0.6},
    "guidance_down":   {"horizon": "7d",  "expected_impact": -0.03, "confidence": 0.6},
    "product_launch":  {"horizon": "7d",  "expected_impact": +0.01, "confidence": 0.3},
    "downgrade":       {"horizon": "1d",  "expected_impact": -0.03, "confidence": 0.6},
    "upgrade":         {"horizon": "1d",  "expected_impact": +0.03, "confidence": 0.6},
    "contract_win":    {"horizon": "7d",  "expected_impact": +0.02, "confidence": 0.5},
}

# Relevance score threshold
RELEVANCE_THRESHOLD: float = 0.4

# Source Reliability Weights (institutional grade)
SOURCE_RELIABILITY_WEIGHTS: dict[str, float] = {
    # Official filings — highest trust
    "sec.gov": 1.0, "idx.co.id": 1.0, "ojk.go.id": 1.0,
    # Major wire services
    "reuters.com": 0.95, "apnews.com": 0.95, "bloomberg.com": 0.95,
    # Financial press
    "wsj.com": 0.90, "ft.com": 0.90, "cnbc.com": 0.85,
    "marketwatch.com": 0.85, "barrons.com": 0.85,
    # Tech/business press
    "techcrunch.com": 0.80, "theverge.com": 0.75,
    # Indonesian financial press
    "kontan.co.id": 0.85, "bisnis.com": 0.85, "cnbcindonesia.com": 0.80,
    # General news
    "yahoo.com": 0.70, "google.com": 0.70,
    # Analyst/blog — lower trust
    "seekingalpha.com": 0.65, "fool.com": 0.60, "investopedia.com": 0.60,
}


def get_source_weight(url: str) -> float:
    """Get reliability weight for a news source URL."""
    if not url:
        return 0.5
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.lower().replace("www.", "")
    for known_domain, weight in SOURCE_RELIABILITY_WEIGHTS.items():
        if known_domain in domain:
            return weight
    return 0.5  # Unknown source default

# ============================================
# Indonesian financial keyword dictionary
# ============================================
POSITIVE_KEYWORDS_ID = [
    "laba", "untung", "naik", "meningkat", "tumbuh", "pertumbuhan",
    "capaian", "positif", "rekor", "optimis", "dividen", "akuisisi",
    "ekspansi", "kerjasama", "inovasi", "pemulihan", "surplus",
    "kenaikan", "berhasil", "peningkatan", "peluang", "profit",
    "menguat", "bullish", "outperform", "prospek", "cerah",
    "melampaui", "meroket", "membaik", "stabil", "unggul",
    "pendapatan", "apresiasi", "breakout", "rally", "reli",
]

NEGATIVE_KEYWORDS_ID = [
    "rugi", "turun", "menurun", "jatuh", "negatif", "gagal",
    "defisit", "krisis", "resesi", "pailit", "bangkrut", "gugatan",
    "sanksi", "denda", "korupsi", "penipuan", "pelanggaran",
    "penurunan", "kerugian", "risiko", "ancaman", "pemutusan",
    "melemah", "bearish", "underperform", "anjlok", "terpuruk",
    "merosot", "tertekan", "volatil", "default", "kredit macet",
    "likuidasi", "moratorium", "suspensi", "downgrade",
]

# ============================================
# English financial keyword dictionary (fallback)
# ============================================
POSITIVE_KEYWORDS_EN = [
    "profit", "growth", "beat", "exceeded", "upgrade", "bullish",
    "rally", "surge", "outperform", "strong", "record", "dividend",
    "expansion", "innovation", "recovery", "optimistic", "positive",
    "breakout", "revenue growth", "earnings beat", "above expectations",
    "upside", "buy", "accumulate", "overweight",
]

NEGATIVE_KEYWORDS_EN = [
    "loss", "decline", "miss", "downgrade", "bearish", "crash",
    "plunge", "underperform", "weak", "default", "bankruptcy",
    "lawsuit", "fraud", "scandal", "layoff", "recession",
    "negative", "risk", "warning", "sell", "below expectations",
    "downside", "reduce", "underweight", "restructuring",
]

# ============================================
# Event tagging keywords (EN + ID)
# ============================================
EVENT_KEYWORDS: dict[str, list[str]] = {
    "earnings_beat": [
        "beat expectations", "exceeded estimates", "above consensus",
        "earnings beat", "better than expected", "melampaui ekspektasi",
        "di atas estimasi", "melampaui konsensus",
    ],
    "earnings_miss": [
        "missed expectations", "below estimates", "below consensus",
        "earnings miss", "worse than expected", "di bawah ekspektasi",
        "di bawah estimasi",
    ],
    "acquisition": [
        "acquisition", "acquired", "merger", "takeover", "buyout",
        "akuisisi", "mengakuisisi", "penggabungan",
    ],
    "dividend": [
        "dividend", "dividen", "pembagian dividen", "dividend yield",
        "dividen interim", "dividen final",
    ],
    "lawsuit": [
        "lawsuit", "sued", "litigation", "legal action",
        "gugatan", "tuntutan hukum", "digugat",
    ],
    "restructuring": [
        "restructuring", "layoff", "cost cutting", "downsizing",
        "restrukturisasi", "pemutusan hubungan kerja", "efisiensi",
    ],
    "ipo": [
        "ipo", "initial public offering", "going public",
        "penawaran umum perdana",
    ],
    "partnership": [
        "partnership", "strategic alliance", "collaboration", "joint venture",
        "kemitraan", "kerjasama", "aliansi strategis",
    ],
    "regulation": [
        "regulation", "regulatory", "compliance", "policy change",
        "regulasi", "kebijakan", "peraturan", "ojk", "bank indonesia",
    ],
    # --- New event types ---
    "guidance_up": [
        "raised guidance", "raised outlook", "increased forecast",
        "upward revision", "raised full-year",
    ],
    "guidance_down": [
        "lowered guidance", "cut forecast", "reduced outlook",
        "downward revision", "lowered full-year", "cut outlook",
    ],
    "product_launch": [
        "product launch", "launched", "new product", "unveiled",
        "release", "peluncuran produk",
    ],
    "downgrade": [
        "downgrade", "downgraded", "cut to sell", "cut to underweight",
        "lowered rating", "price target cut",
    ],
    "upgrade": [
        "upgrade", "upgraded", "raised to buy", "raised to overweight",
        "raised rating", "price target raised",
    ],
    "contract_win": [
        "contract win", "awarded contract", "won contract", "new deal",
        "secured deal", "won deal", "kontrak baru",
    ],
    # --- Indonesian-specific events ---
    "rups": [
        "rups", "rapat umum pemegang saham", "annual general meeting",
        "extraordinary general meeting", "rupst", "rupslb",
    ],
    "right_issue": [
        "right issue", "rights issue", "hak memesan efek terlebih dahulu",
        "hmetd", "penambahan modal",
    ],
    "stock_split": [
        "stock split", "reverse split", "pemecahan saham",
        "penggabungan saham",
    ],
    "buyback": [
        "buyback", "share buyback", "stock repurchase",
        "pembelian kembali saham",
    ],
}

# ============================================
# Indonesian stopwords for language detection
# ============================================
INDONESIAN_STOPWORDS = {
    "dan", "yang", "di", "dari", "untuk", "dengan", "pada",
    "ini", "itu", "adalah", "ke", "akan", "oleh", "tidak",
    "juga", "sudah", "telah", "dalam", "ada", "atau", "bisa",
    "sebagai", "dapat", "bahwa", "lebih", "tahun",
    "tersebut", "kata", "saat", "masih", "hingga", "namun",
    "tetapi", "serta", "karena", "antara", "setelah",
    "seperti", "menjadi", "secara", "yakni", "agar",
}


def is_indonesian(text: str) -> bool:
    """Detect if text is Indonesian using stopword + keyword heuristic."""
    words = set(text.lower().split())
    id_stopword_count = len(words.intersection(INDONESIAN_STOPWORDS))

    # Also check for Indonesian financial keywords
    id_keyword_count = sum(1 for w in words if w in POSITIVE_KEYWORDS_ID or w in NEGATIVE_KEYWORDS_ID)

    # Indonesian if 3+ stopwords OR 2+ Indonesian financial keywords
    return id_stopword_count >= 3 or id_keyword_count >= 2


def analyze_sentiment_finbert(text: str) -> tuple[str, float]:
    """
    Analyze sentiment using FinBERT (English financial text).

    Returns:
        Tuple of (sentiment_label, confidence_score)
    """
    try:
        from transformers import pipeline

        classifier = pipeline(
            "sentiment-analysis",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
        )

        # Truncate to 512 tokens
        result = classifier(text[:512])[0]

        label = result["label"].lower()
        score = result["score"]

        # Map FinBERT labels
        if label == "positive":
            return "positive", score
        elif label == "negative":
            return "negative", score
        else:
            return "neutral", score

    except Exception as e:
        logger.info(f"FinBERT not available, using English keyword fallback: {e}")
        return analyze_sentiment_keyword_en(text)


def analyze_sentiment_keyword(text: str) -> tuple[str, float]:
    """
    Analyze sentiment using Indonesian keyword dictionary.

    Returns:
        Tuple of (sentiment_label, confidence_score)
    """
    text_lower = text.lower()
    words = text_lower.split()

    pos_count = sum(1 for w in words if w in POSITIVE_KEYWORDS_ID)
    neg_count = sum(1 for w in words if w in NEGATIVE_KEYWORDS_ID)

    # Also check multi-word phrases
    for phrase in POSITIVE_KEYWORDS_ID:
        if " " in phrase and phrase in text_lower:
            pos_count += 1
    for phrase in NEGATIVE_KEYWORDS_ID:
        if " " in phrase and phrase in text_lower:
            neg_count += 1

    total = pos_count + neg_count
    if total == 0:
        return "neutral", 0.5

    if pos_count > neg_count:
        confidence = pos_count / total
        return "positive", min(confidence, 0.95)
    elif neg_count > pos_count:
        confidence = neg_count / total
        return "negative", min(confidence, 0.95)
    else:
        return "neutral", 0.5


def analyze_sentiment_keyword_en(text: str) -> tuple[str, float]:
    """
    Analyze sentiment using English keyword dictionary.
    Fallback when FinBERT/transformers is not available.

    Returns:
        Tuple of (sentiment_label, confidence_score)
    """
    text_lower = text.lower()
    words = text_lower.split()

    pos_count = sum(1 for w in words if w in POSITIVE_KEYWORDS_EN)
    neg_count = sum(1 for w in words if w in NEGATIVE_KEYWORDS_EN)

    # Also check multi-word phrases
    for phrase in POSITIVE_KEYWORDS_EN:
        if " " in phrase and phrase in text_lower:
            pos_count += 1
    for phrase in NEGATIVE_KEYWORDS_EN:
        if " " in phrase and phrase in text_lower:
            neg_count += 1

    total = pos_count + neg_count
    if total == 0:
        return "neutral", 0.5

    if pos_count > neg_count:
        confidence = pos_count / total
        return "positive", min(confidence, 0.95)
    elif neg_count > pos_count:
        confidence = neg_count / total
        return "negative", min(confidence, 0.95)
    else:
        return "neutral", 0.5


def tag_events(text: str) -> list[dict]:
    """
    Tag events from text using keyword matching.
    Returns list of event dicts with type, horizon, expected_impact, confidence.
    """
    text_lower = text.lower()
    events = []

    for event_type, keywords in EVENT_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_lower:
                impact_info = EVENT_IMPACT_MODEL.get(event_type, {})
                events.append({
                    "event_type": event_type,
                    "horizon": impact_info.get("horizon", "7d"),
                    "expected_impact": impact_info.get("expected_impact", 0.0),
                    "confidence": impact_info.get("confidence", 0.3),
                })
                break  # Only tag each event type once

    return events


def tag_events_simple(text: str) -> list[str]:
    """Tag events from text — returns simple list of event type strings (legacy compat)."""
    return [e["event_type"] for e in tag_events(text)]


def analyze_news_item(
    title: str,
    body: Optional[str] = None,
) -> dict:
    """
    Analyze a single news item for sentiment and events.

    Args:
        title: News headline
        body: Optional article body

    Returns:
        Dict with sentiment, impact, events, event_details
    """
    full_text = title
    if body:
        full_text = f"{title}. {body}"

    # Detect language and choose analyzer
    if is_indonesian(full_text):
        sentiment, confidence = analyze_sentiment_keyword(full_text)
    else:
        sentiment, confidence = analyze_sentiment_finbert(full_text)

    # Tag events (rich format)
    event_details = tag_events(full_text)
    event_names = [e["event_type"] for e in event_details]

    # Calculate impact score (higher for strong sentiment with events)
    impact = confidence
    if event_details:
        # Use max expected impact from detected events to boost
        max_event_impact = max(abs(e["expected_impact"]) for e in event_details)
        impact = min(impact * (1.0 + max_event_impact * 5), 1.0)

    return {
        "sentiment": sentiment,
        "impact": round(impact, 4),
        "events": event_names,
        "event_details": event_details,
    }


def _is_blacklisted(text: str) -> bool:
    """Check if text matches any blacklist spam pattern."""
    text_lower = text.lower()
    return any(pattern in text_lower for pattern in BLACKLIST_PATTERNS)


def compute_relevance_score(
    text: str,
    title: str,
    ticker: str,
    company_names: list[str],
) -> tuple[float, str]:
    """
    Compute relevance score (0.0-1.0) for a news item.

    Scoring:
      +0.40 base if any company name/alias found in text
      +0.30 bonus if ticker symbol found in title
      +0.20 bonus if company name in title (vs body only)
      +0.10 bonus if no blacklist patterns found nearby
      -1.00 if blacklisted (returns 0.0)

    Returns:
        Tuple of (score, reason_string)
    """
    if not text:
        return 0.0, "empty text"

    # Blacklist rejection (instant 0)
    if _is_blacklisted(title):
        return 0.0, "blacklisted title pattern"

    text_lower = text.lower()
    title_lower = title.lower()
    score = 0.0
    reasons = []

    # Base: any company name/alias match anywhere
    alias_matched = None
    for name in company_names:
        if name in text_lower:
            alias_matched = name
            score += 0.40
            reasons.append(f"alias '{name}' in text")
            break

    if alias_matched is None:
        return 0.0, "no alias match"

    # Bonus: ticker symbol in title
    base_ticker = ticker.split(".")[0].upper()
    ticker_patterns = [base_ticker, ticker.upper()]
    for tp in ticker_patterns:
        # Use word boundary check to avoid partial matches
        if re.search(r'\b' + re.escape(tp) + r'\b', title, re.IGNORECASE):
            score += 0.30
            reasons.append(f"ticker '{tp}' in title")
            break

    # Bonus: company name in title (not just body)
    for name in company_names:
        if len(name) > 2 and name in title_lower:
            score += 0.20
            reasons.append(f"alias '{name}' in title")
            break

    # Bonus: no blacklist nearby
    if not _is_blacklisted(text):
        score += 0.10
        reasons.append("no blacklist in body")

    return min(score, 1.0), "; ".join(reasons)


def _dedup_news_by_title(items: list[dict], threshold: float = 0.80) -> list[dict]:
    """
    Deduplicate news items by title similarity.
    Keeps the earliest item per cluster.
    """
    from ..collectors.news_rss import title_similarity

    if not items:
        return []

    # Sort by published date (earliest first)
    sorted_items = sorted(
        items,
        key=lambda x: x.get("published_at") or "",
    )

    kept: list[dict] = []
    kept_titles: list[str] = []

    for item in sorted_items:
        title = item.get("title", "")
        is_dup = False
        for kt in kept_titles:
            if title_similarity(title, kt) >= threshold:
                is_dup = True
                break
        if not is_dup:
            kept.append(item)
            kept_titles.append(title)

    deduped = len(items) - len(kept)
    if deduped > 0:
        logger.info(f"Dedup: removed {deduped} duplicate titles, kept {len(kept)}")
    return kept


def compute_weighted_sentiment(results: list[dict]) -> dict:
    """
    Compute time-weighted sentiment summary.
    Recent news weighted more (half-life ~7 days).

    Returns:
        Dict with weighted_score (-1 to +1), pos/neg/neu counts, decay info.
    """
    if not results:
        return {"weighted_score": 0.0, "positive": 0, "negative": 0, "neutral": 0}

    now = datetime.now(timezone.utc)
    total_weight = 0.0
    weighted_sum = 0.0
    pos = neg = neu = 0

    for r in results:
        # Parse date
        pub_date = r.get("date")
        if pub_date is None:
            age_days = 7.0  # default
        elif isinstance(pub_date, datetime):
            age_days = max(0, (now - pub_date.replace(tzinfo=timezone.utc if pub_date.tzinfo is None else pub_date.tzinfo)).total_seconds() / 86400)
        else:
            age_days = 7.0

        # Exponential decay weight (half-life = 7 days)
        weight = math.exp(-age_days / 7.0)

        # Sentiment value
        sentiment = r.get("sentiment", "neutral")
        impact = float(r.get("impact", 0.5))

        if sentiment == "positive":
            weighted_sum += weight * impact
            pos += 1
        elif sentiment == "negative":
            weighted_sum -= weight * impact
            neg += 1
        else:
            neu += 1

        total_weight += weight

    weighted_score = weighted_sum / total_weight if total_weight > 0 else 0.0

    return {
        "weighted_score": round(weighted_score, 4),
        "positive": pos,
        "negative": neg,
        "neutral": neu,
        "total_weight": round(total_weight, 2),
    }


def run_news_sentiment(ticker: str) -> list[dict]:
    """
    Run sentiment analysis on company-relevant, unanalyzed news (last 14 days).

    Includes:
    - Blacklist filtering (generic market research spam)
    - Relevance scoring (0-1) with threshold
    - Title deduplication
    - Sentiment decay weighting
    - Precision metrics

    Args:
        ticker: Stock ticker

    Returns:
        List of analyzed results (only company-relevant items)
    """
    logger.info(f"Running news sentiment analysis for {ticker}")

    # Get unanalyzed news items (last 14 days)
    news_items = _get_unanalyzed_news(ticker)

    if not news_items:
        logger.info(f"No unanalyzed news found for {ticker}")
        return []

    # Resolve company names for relevance check
    company_names = _resolve_company_names(ticker)
    logger.info(f"Company names for {ticker}: {company_names}")

    # Step 1: Blacklist filter
    blacklisted = 0
    non_blacklisted = []
    for item in news_items:
        if _is_blacklisted(item.get("title", "")):
            blacklisted += 1
        else:
            non_blacklisted.append(item)

    if blacklisted > 0:
        logger.info(f"Blacklist filter: removed {blacklisted} spam items")

    # Step 2: Relevance scoring + filtering
    relevant_items = []
    skipped = 0
    hard_keyword_matches = 0

    for item in non_blacklisted:
        title = item.get("title", "")
        text = title
        if item.get("body"):
            text += " " + item["body"]

        rel_score, reason = compute_relevance_score(text, title, ticker, company_names)

        if rel_score >= RELEVANCE_THRESHOLD:
            item["metadata"] = {
                "relevance_reason": reason,
                "relevance_score": rel_score,
            }
            relevant_items.append(item)

            # Track hard keyword match (ticker symbol in title/text)
            base_ticker = ticker.split(".")[0].upper()
            if re.search(r'\b' + re.escape(base_ticker) + r'\b', text, re.IGNORECASE):
                hard_keyword_matches += 1
        else:
            skipped += 1

    logger.info(
        f"Relevance filter: {len(news_items)} total -> "
        f"{blacklisted} blacklisted, {skipped} low-relevance, "
        f"{len(relevant_items)} relevant"
    )

    # Precision metric
    if relevant_items:
        precision = hard_keyword_matches / len(relevant_items)
        logger.info(
            f"news_relevance_precision: {precision:.1%} "
            f"({hard_keyword_matches}/{len(relevant_items)} contain hard keyword)"
        )

    if not relevant_items:
        logger.info(f"No company-relevant news found for {ticker}")
        return []

    # Step 3: Dedup by title similarity
    relevant_items = _dedup_news_by_title(relevant_items)

    # Step 4: Analyze sentiment for each relevant item
    results = []

    for item in relevant_items:
        analysis = analyze_news_item(
            title=item["title"],
            body=item.get("body"),
        )

        # Add relevance info to events for auditability
        relevance_reason = item.get("metadata", {}).get("relevance_reason", "")
        rel_score = item.get("metadata", {}).get("relevance_score", 0.0)
        analysis["events"].append(f"RELEVANCE: {relevance_reason} (score={rel_score:.2f})")

        result = {
            "ticker": ticker,
            "date": item.get("published_at"),
            "headline": item["title"],
            "sentiment": analysis["sentiment"],
            "impact": analysis["impact"],
            "events_json": analysis["events"],
            "sources_json": [{
                "title": item["title"],
                "url": item["url"],
                "source": item["source"],
                "date": str(item.get("published_at", "")),
            }],
        }
        results.append(result)

    # Step 5: Compute weighted sentiment summary
    sentiment_summary = compute_weighted_sentiment(results)
    logger.info(
        f"Sentiment analysis complete: "
        f"{sentiment_summary['positive']} positive, "
        f"{sentiment_summary['negative']} negative, "
        f"{sentiment_summary['neutral']} neutral | "
        f"weighted_score={sentiment_summary['weighted_score']:+.4f}"
    )

    return results


def _get_unanalyzed_news(ticker: str) -> list[dict]:
    """Get news items from last 14 days that haven't been sentiment-analyzed yet."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT ni.id, ni.ticker, ni.source, ni.published_at,
                   ni.title, ni.url, ni.body
            FROM news_items ni
            WHERE ni.ticker = %(ticker)s
              AND ni.published_at >= NOW() - INTERVAL '14 days'
              AND NOT EXISTS (
                  SELECT 1 FROM news_sentiment ns
                  WHERE ns.ticker = ni.ticker
                    AND ns.headline = ni.title
              )
            ORDER BY ni.published_at DESC
            LIMIT 500
            """,
            {"ticker": ticker},
        )
        return cursor.fetchall()


# ============================================
# Company name/alias map for relevance filtering
# ============================================
COMPANY_ALIASES: dict[str, list[str]] = {
    "BBCA": ["bca", "bank central asia", "bank bca"],
    "BBRI": ["bri", "bank rakyat indonesia", "bank bri"],
    "BMRI": ["mandiri", "bank mandiri"],
    "BBNI": ["bni", "bank negara indonesia", "bank bni"],
    "TLKM": ["telkom", "telekomunikasi indonesia", "telkomsel"],
    "ASII": ["astra", "astra international"],
    "UNVR": ["unilever", "unilever indonesia"],
    "GOTO": ["goto", "gojek", "tokopedia", "gojek tokopedia"],
    "BRIS": ["bsi", "bank syariah indonesia"],
    "ICBP": ["indofood cbp", "indofood", "icbp"],
    "INDF": ["indofood", "indofood sukses"],
    "KLBF": ["kalbe", "kalbe farma"],
    "PGAS": ["pgn", "perusahaan gas negara"],
    "SMGR": ["semen indonesia", "semen gresik"],
    "ANTM": ["antam", "aneka tambang"],
    "AAPL": ["apple"],
    "MSFT": ["microsoft"],
    "GOOGL": ["google", "alphabet"],
    "AMZN": ["amazon"],
    "TSLA": ["tesla"],
    "META": ["meta", "facebook"],
    "NVDA": ["nvidia"],
    "ORCL": ["oracle", "oci", "larry ellison"],
    "CRM": ["salesforce"],
    "INTC": ["intel"],
    "AMD": ["advanced micro devices", "amd"],
    "IBM": ["ibm", "international business machines"],
}

# Cache for resolved company names
_company_name_cache: dict[str, list[str]] = {}


def _resolve_company_names(ticker: str) -> list[str]:
    """
    Resolve company names for a ticker using aliases + yfinance.
    Results are cached per ticker.
    """
    if ticker in _company_name_cache:
        return _company_name_cache[ticker]

    base_ticker = ticker.split(".")[0].upper()
    names: list[str] = [base_ticker.lower(), ticker.lower()]

    # Add known aliases
    if base_ticker in COMPANY_ALIASES:
        names.extend(COMPANY_ALIASES[base_ticker])

    # Try yfinance for additional names
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        for key in ("shortName", "longName"):
            name = info.get(key)
            if name:
                # Add full name and first word (usually company brand)
                names.append(name.lower())
                brand = name.split()[0].lower()
                if len(brand) > 2:
                    names.append(brand)
    except Exception as e:
        logger.debug(f"yfinance lookup for company names failed: {e}")

    # Deduplicate
    names = list(dict.fromkeys(names))
    _company_name_cache[ticker] = names
    return names


def is_relevant_to_company(
    text: str,
    ticker: str,
    company_names: list[str] | None = None,
) -> Optional[str]:
    """
    Check if a news text is relevant to a specific company.

    Uses blacklist filtering + relevance scoring.

    Args:
        text: News headline + body
        ticker: Stock ticker
        company_names: Pre-resolved company names (optional, will resolve if None)

    Returns:
        Match reason string if relevant, None otherwise.
    """
    if not text:
        return None

    if company_names is None:
        company_names = _resolve_company_names(ticker)

    # Extract title (first sentence or first 200 chars)
    title = text.split(".")[0][:200] if "." in text[:200] else text[:200]

    score, reason = compute_relevance_score(text, title, ticker, company_names)
    if score >= RELEVANCE_THRESHOLD:
        return f"Relevance {score:.2f}: {reason}"

    return None

