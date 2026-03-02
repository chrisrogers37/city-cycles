"""Biking score display -- large number with color-coded condition ring."""

import streamlit as st


def _score_color(score: int) -> str:
    """Return hex color based on score (0-100)."""
    if score >= 80:
        return '#2ECC71'   # green
    elif score >= 60:
        return '#F39C12'   # amber
    elif score >= 40:
        return '#E67E22'   # orange
    else:
        return '#E74C3C'   # red


def render_biking_score(score: int, label: str = '') -> None:
    """Render the biking score as a styled metric."""
    color = _score_color(score)
    label_html = f"<p style='font-size: 0.8rem; color: rgba(255,255,255,0.6); margin: 0.3rem 0 0 0;'>{label}</p>" if label else ""
    st.markdown(f"""
    <div style="text-align: center;">
        <p style="font-size: 0.85rem; color: rgba(255,255,255,0.6); margin: 0 0 0.3rem 0;
           text-transform: uppercase; letter-spacing: 0.1em;">Biking Score</p>
        <p style="font-size: 3.5rem; font-weight: 700; margin: 0; color: {color};
           text-shadow: 0 0 20px {color}40;">{score}</p>
        <p style="font-size: 0.75rem; color: rgba(255,255,255,0.5); margin: 0;">out of 100</p>
        {label_html}
    </div>
    """, unsafe_allow_html=True)
