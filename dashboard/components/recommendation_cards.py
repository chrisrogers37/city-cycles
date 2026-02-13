"""Render recommendation insight cards from Phase 04 engine."""

import streamlit as st
from dashboard.recommendation_engine import Severity, RecommendationResult

_SEVERITY_EMOJI = {
    Severity.POSITIVE: "&#9989;",
    Severity.NEUTRAL: "&#8505;",
    Severity.CAUTION: "&#9888;",
    Severity.WARNING: "&#128721;",
}

_SEVERITY_BG_COLOR = {
    Severity.POSITIVE: "rgba(46, 204, 113, 0.15)",
    Severity.NEUTRAL: "rgba(255, 255, 255, 0.08)",
    Severity.CAUTION: "rgba(243, 156, 18, 0.15)",
    Severity.WARNING: "rgba(231, 76, 60, 0.15)",
}

CARD_STYLE = (
    "padding:0.75em 1em; border-radius:10px; "
    "margin-bottom:0.5em; font-size:0.9em; "
    "border: 1px solid rgba(255,255,255,0.08); "
    "backdrop-filter: blur(8px);"
)


def render_recommendations(result: RecommendationResult) -> None:
    """Render biking score and recommendation cards."""
    score = result.biking_score
    st.markdown(
        f"<div style='text-align:center; padding:0.75em; "
        f"background:{score.color}22; border-radius:10px; margin-bottom:0.75em; "
        f"border: 1px solid rgba(255,255,255,0.08);'>"
        f"<h3 style='margin:0; color:{score.color};'>"
        f"Biking Score: {score.score}/100</h3>"
        f"<p style='margin:0; color: rgba(255,255,255,0.7);'>{score.label}</p>"
        f"</div>",
        unsafe_allow_html=True,
    )
    for rec in result.recommendations:
        emoji = _SEVERITY_EMOJI[rec.severity]
        bg = _SEVERITY_BG_COLOR[rec.severity]
        st.markdown(
            f"<div style='{CARD_STYLE} background:{bg};'>"
            f"{emoji} {rec.text}</div>",
            unsafe_allow_html=True,
        )
