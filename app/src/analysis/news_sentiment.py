"""
News Sentiment module for Finance Analytics.
Analyzes news articles using FinBERT (English) or keyword dictionary (Indonesian).
Includes event tagging via keyword/rule matching.
"""

import logging
import re
from typing import Optional

from ..db import get_db_cursor

logger = logging.getLogger(__name__)

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


def tag_events(text: str) -> list[str]:
    """
    Tag events from text using keyword matching.

    Returns:
        List of detected event tags
    """
    text_lower = text.lower()
    events = []

    for event_type, keywords in EVENT_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text_lower:
                events.append(event_type)
                break  # Only tag each event type once

    return events


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
        Dict with sentiment, impact, events
    """
    full_text = title
    if body:
        full_text = f"{title}. {body}"

    # Detect language and choose analyzer
    if is_indonesian(full_text):
        sentiment, confidence = analyze_sentiment_keyword(full_text)
    else:
        sentiment, confidence = analyze_sentiment_finbert(full_text)

    # Tag events
    events = tag_events(full_text)

    # Calculate impact score (higher for strong sentiment with events)
    impact = confidence
    if events:
        impact = min(impact * 1.2, 1.0)  # Boost if events detected

    return {
        "sentiment": sentiment,
        "impact": round(impact, 4),
        "events": events,
    }


def run_news_sentiment(ticker: str) -> list[dict]:
    """
    Run sentiment analysis on company-relevant, unanalyzed news (last 14 days).

    Only analyzes news that is relevant to the specific company identified
    by the ticker (checks ticker, company name, aliases in headline/body).

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

    # Filter to company-relevant news only
    relevant_items = []
    skipped = 0
    for item in news_items:
        text = item["title"]
        if item.get("body"):
            text += " " + item["body"]
        
        match_reason = is_relevant_to_company(text, ticker, company_names)
        if match_reason:
            item["metadata"] = {"relevance_reason": match_reason}
            relevant_items.append(item)
        else:
            skipped += 1

    logger.info(
        f"Filtered {len(news_items)} news items -> "
        f"{len(relevant_items)} relevant, {skipped} skipped"
    )

    if not relevant_items:
        logger.info(f"No company-relevant news found for {ticker}")
        return []

    results = []

    for item in relevant_items:
        analysis = analyze_news_item(
            title=item["title"],
            body=item.get("body"),
        )

        # Add relevance reason to events for auditability
        relevance_reason = item.get("metadata", {}).get("relevance_reason")
        if relevance_reason:
            analysis["events"].append(f"RELEVANCE: {relevance_reason}")

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

    logger.info(
        f"Sentiment analysis complete: "
        f"{sum(1 for r in results if r['sentiment'] == 'positive')} positive, "
        f"{sum(1 for r in results if r['sentiment'] == 'negative')} negative, "
        f"{sum(1 for r in results if r['sentiment'] == 'neutral')} neutral"
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

    Looks for ticker symbols, company names, and aliases in the text.

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

    text_lower = text.lower()

    for name in company_names:
        if name in text_lower:
            return f"Matched alias '{name}'"

    return None

