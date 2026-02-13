"""Tests for dashboard/components/chart_factory.py."""

import pytest
import pandas as pd


class TestChartFactory:
    """Test the chart factory creates valid Plotly figures."""

    def test_monthly_trend_chart_returns_figure(self):
        """monthly_trend_chart should return a Plotly Figure."""
        from dashboard.theme.plotly_template import register_template
        from dashboard.components.chart_factory import monthly_trend_chart
        import plotly.graph_objects as go

        register_template()
        df = pd.DataFrame({
            'month': [1, 2, 3, 1, 2, 3],
            'year': [2023, 2023, 2023, 2024, 2024, 2024],
            'metric_value': [100, 200, 300, 150, 250, 350],
        })
        fig = monthly_trend_chart(df, 'metric_value', 'Rides', 'Test Chart')
        assert isinstance(fig, go.Figure)

    def test_monthly_trend_chart_has_month_labels(self):
        """monthly_trend_chart should set month text labels on x-axis."""
        from dashboard.theme.plotly_template import register_template
        from dashboard.components.chart_factory import monthly_trend_chart, MONTH_LABELS

        register_template()
        df = pd.DataFrame({
            'month': [1, 2], 'year': [2023, 2023], 'metric_value': [100, 200],
        })
        fig = monthly_trend_chart(df, 'metric_value', 'Rides', 'Test')
        xaxis = fig.layout.xaxis
        assert list(xaxis.ticktext) == MONTH_LABELS

    def test_hourly_bar_chart_returns_figure(self):
        """hourly_bar_chart should return a Plotly Figure."""
        from dashboard.theme.plotly_template import register_template
        from dashboard.components.chart_factory import hourly_bar_chart
        import plotly.graph_objects as go

        register_template()
        df = pd.DataFrame({
            'hour_of_day': list(range(24)),
            'ride_count': [i * 100 for i in range(24)],
        })
        fig = hourly_bar_chart(df, 'Test Hourly')
        assert isinstance(fig, go.Figure)

    def test_comparison_line_chart_returns_figure(self):
        """comparison_line_chart should return a Plotly Figure."""
        from dashboard.theme.plotly_template import register_template
        from dashboard.components.chart_factory import comparison_line_chart
        import plotly.graph_objects as go

        register_template()
        df = pd.DataFrame({
            'period': ['2023-01', '2023-02', '2023-01', '2023-02'],
            'metric_value': [100, 200, 80, 150],
            'location': ['nyc', 'nyc', 'london', 'london'],
        })
        fig = comparison_line_chart(df, 'period', 'metric_value', 'Test', 'Rides', 'Period')
        assert isinstance(fig, go.Figure)

    def test_grouped_bar_chart_returns_figure(self):
        """grouped_bar_chart should return a Plotly Figure."""
        from dashboard.theme.plotly_template import register_template
        from dashboard.components.chart_factory import grouped_bar_chart
        import plotly.graph_objects as go

        register_template()
        df = pd.DataFrame({
            'year': [2023, 2023], 'station_count': [100, 80],
            'location': ['nyc', 'london'],
        })
        fig = grouped_bar_chart(df, 'year', 'station_count', 'Test', 'Stations')
        assert isinstance(fig, go.Figure)

    def test_station_growth_chart_returns_figure(self):
        """station_growth_chart should return a Plotly Figure."""
        from dashboard.theme.plotly_template import register_template
        from dashboard.components.chart_factory import station_growth_chart
        import plotly.graph_objects as go

        register_template()
        df = pd.DataFrame({'year': [2023, 2024], 'metric_value': [100, 120]})
        fig = station_growth_chart(df, 'Test Growth')
        assert isinstance(fig, go.Figure)
