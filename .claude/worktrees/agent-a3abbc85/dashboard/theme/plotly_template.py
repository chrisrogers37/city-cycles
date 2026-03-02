"""
Atmospheric Plotly chart template.
Dark background, weather-aware colors, consistent typography.
"""

import plotly.graph_objects as go
import plotly.io as pio

ATMOSPHERIC_COLORS = [
    '#5DADE2',  # sky blue
    '#E74C3C',  # warm red
    '#2ECC71',  # green
    '#F39C12',  # amber
    '#9B59B6',  # purple
    '#1ABC9C',  # teal
    '#E67E22',  # orange
    '#3498DB',  # darker blue
]

RAIN_COLORS = ['#2C3E50', '#5DADE2', '#85C1E9', '#AED6F1', '#D6EAF8']
SUN_COLORS = ['#F39C12', '#E74C3C', '#E67E22', '#F5B041', '#FAD7A0']


def create_atmospheric_template() -> go.layout.Template:
    """Create a Plotly template matching the atmospheric dashboard theme."""
    template = go.layout.Template()

    template.layout = go.Layout(
        paper_bgcolor='rgba(0, 0, 0, 0)',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        font=dict(
            family='-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
            color='rgba(255, 255, 255, 0.8)',
            size=13,
        ),
        title=dict(
            font=dict(size=18, color='#FFFFFF'),
            x=0.0,
        ),
        xaxis=dict(
            gridcolor='rgba(255, 255, 255, 0.06)',
            linecolor='rgba(255, 255, 255, 0.1)',
            zerolinecolor='rgba(255, 255, 255, 0.1)',
        ),
        yaxis=dict(
            gridcolor='rgba(255, 255, 255, 0.06)',
            linecolor='rgba(255, 255, 255, 0.1)',
            zerolinecolor='rgba(255, 255, 255, 0.1)',
        ),
        colorway=ATMOSPHERIC_COLORS,
        legend=dict(
            bgcolor='rgba(0, 0, 0, 0)',
            font=dict(color='rgba(255, 255, 255, 0.7)'),
        ),
        hoverlabel=dict(
            bgcolor='rgba(20, 25, 35, 0.9)',
            font_color='#FFFFFF',
            bordercolor='rgba(255, 255, 255, 0.1)',
        ),
    )

    return template


def register_template() -> None:
    """Register the atmospheric template as 'atmospheric' in Plotly's registry."""
    pio.templates['atmospheric'] = create_atmospheric_template()
    pio.templates.default = 'atmospheric'
