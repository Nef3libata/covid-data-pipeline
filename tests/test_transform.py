import pandas as pd
from covid_pipeline.transform import transform_covid_data
from covid_pipeline.quality import DataValidationError


def test_basic_transform(tmp_path):
    data = {
        "dateRep": ["01/01/2021", "02/01/2021"],
        "day": ["01", "02"],
        "month": ["01", "01"],
        "year": ["2021", "2021"],
        "cases": [10, 20],
        "deaths": [0, 1],
        "countriesAndTerritories": ["Testland", "Testland"],
        "geoId": ["TL", "TL"],
        "countryterritoryCode": ["TST", "TST"],
        "popData2020": [100000, 100000],
        "continentExp": ["Europe", "Europe"],
    }
    df = pd.DataFrame(data)
    out = transform_covid_data(df)
    assert "cases_per_100k" in out.columns
    assert out["cases_per_100k"].iloc[0] == 10 / 100000 * 100000


def test_negative_cases_are_clipped():
    import pandas as pd

    data = {
        "dateRep": ["01/01/2021"],
        "cases": [-5],
        "deaths": [-2],
        "countriesAndTerritories": ["A"],
        "popData2020": [1000],
    }

    df = pd.DataFrame(data)

    out = transform_covid_data(df)

    assert out["cases"].iloc[0] == 0
    assert out["deaths"].iloc[0] == 0
