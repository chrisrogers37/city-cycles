"""Compact horizontal forecast chart for next 24 hours."""

import streamlit as st
import plotly.graph_objects as go


def render_forecast_strip(forecast_entries: list) -> None:
    """Render a compact 24-hour forecast strip chart.

    Args:
        forecast_entries: List of HourlyForecastEntry from weather_service.
    """
    if not forecast_entries:
        st.info("Forecast data unavailable.")
        return

    # Limit to 24 hours
    entries = forecast_entries[:24]
    hours = [e.time.strftime('%H:%M') for e in entries]
    temps = [e.temperature_c for e in entries]
    precip = [e.precipitation_mm for e in entries]

    fig = go.Figure()

    # Precipitation bars (background)
    fig.add_trace(go.Bar(
        x=hours,
        y=precip,
        name='Precip (mm)',
        marker_color='rgba(100, 150, 220, 0.3)',
        yaxis='y2',
    ))

    # Temperature line (foreground)
    fig.add_trace(go.Scatter(
        x=hours,
        y=temps,
        name='Temp (\u00b0C)',
        mode='lines+markers',
        line=dict(color='#FFFFFF', width=2),
        marker=dict(size=6, color='#FFFFFF'),
    ))

    fig.update_layout(
        height=200,
        margin=dict(l=40, r=20, t=10, b=30),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='rgba(255,255,255,0.7)',
        showlegend=False,
        yaxis=dict(title='', gridcolor='rgba(255,255,255,0.05)'),
        yaxis2=dict(overlaying='y', side='right', showgrid=False, showticklabels=False),
        xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
    )

    st.plotly_chart(fig, use_container_width=True)
