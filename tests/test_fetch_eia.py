import pytest
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from ingestion.fetch_eia import fetch_wti_prices, save_raw_response


class TestFetchWtiPrices:

    def test_raises_on_empty_response(self):
        """API returning empty data should raise ValueError"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'response': {'data': []}
        }
        mock_response.raise_for_status = MagicMock()

        with patch('ingestion.fetch_eia.requests.get', return_value=mock_response):
            with pytest.raises(ValueError, match="empty data"):
                fetch_wti_prices("2025-01-01", "2025-01-31")

    def test_returns_dict_on_valid_response(self):
        """Valid API response should return a dictionary"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'response': {
                'data': [
                    {'period': '2025-01-02', 'value': '70.5', 'units': '$/BBL'}
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()

        with patch('ingestion.fetch_eia.requests.get', return_value=mock_response):
            result = fetch_wti_prices("2025-01-01", "2025-01-31")
            assert isinstance(result, dict)
            assert 'response' in result


class TestSaveRawResponse:

    def test_creates_file_with_correct_name(self, tmp_path):
        """Save function should create a JSON file with the correct name"""
        data = {'response': {'data': [{'period': '2025-01-02', 'value': '70.5'}]}}
        fetch_date = "2025-01-31"

        with patch('ingestion.fetch_eia.Path') as mock_path:
            mock_folder = MagicMock()
            mock_path.return_value = mock_folder
            mock_folder.__truediv__ = MagicMock(return_value=tmp_path / f"raw_{fetch_date}.json")
            mock_folder.mkdir = MagicMock()

            real_folder = tmp_path / "eia_oil_prices"
            real_folder.mkdir(parents=True, exist_ok=True)

            import ingestion.fetch_eia as eia_module
            original_path = eia_module.Path

            def fake_path(p):
                if "bronze" in str(p):
                    return real_folder
                return original_path(p)

            with patch('ingestion.fetch_eia.Path', side_effect=fake_path):
                result = save_raw_response(data, fetch_date)
                assert f"raw_{fetch_date}.json" in result