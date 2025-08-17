from dataclasses import dataclass
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

import weaveflow as wf
from weaveflow.options import set_weaveflow_option

# Set main data pool path and specify which files to include
set_weaveflow_option(
    ["asset_path", "include_spool"],
    [Path(__file__).parent / "assets/data", "registry"],
)


def generate_data(seed: int = 42, num_companies: int = 50):
    # Generate random data
    np.random.seed(seed)
    industries = ["Technology", "Healthcare", "Industrials", "Consumer Goods"]
    company_names = [f"Company_{i+1}" for i in range(num_companies)]
    tickers = [f"CMP{i+1}" for i in range(num_companies)]

    data = {
        "company_name": company_names,
        "ticker": tickers,
        "industry": np.random.choice(industries, num_companies, p=[0.3, 0.2, 0.2, 0.3]),
        "current_price": np.round(np.random.uniform(50, 500, num_companies), 1),
        "market_cap_billions": np.round(np.random.uniform(10, 800, num_companies), 1),
        "pe_ratio": np.round(np.random.uniform(15, 60, num_companies), 1),
        "pb_ratio": np.round(np.random.uniform(2, 15, num_companies), 1),
        "ps_ratio": np.round(np.random.uniform(1, 10, num_companies), 1),
        "dividend_yield": np.round(np.random.uniform(0, 0.05, num_companies), 3),
        "payout_ratio": np.round(np.random.uniform(0.1, 0.7, num_companies), 2),
        "free_cash_flow_millions": np.round(
            np.random.uniform(100, 5000, num_companies), 1
        ),
        "revenue_growth_rate": np.round(
            np.random.uniform(-0.02, 0.25, num_companies), 3
        ),
        "debt_to_equity": np.round(np.random.uniform(0.1, 2.5, num_companies), 2),
    }
    companies_df = pd.DataFrame(data)

    # Make data more realistic
    # Tech companies often have higher growth and P/E but lower dividends
    tech_mask = companies_df["industry"] == "Technology"
    companies_df.loc[tech_mask, "revenue_growth_rate"] *= 1.5
    companies_df.loc[tech_mask, "pe_ratio"] *= 1.3
    companies_df.loc[tech_mask, ["dividend_yield", "payout_ratio"]] = 0.0

    # Consumer goods are more stable, lower growth, but pay dividends
    consumer_mask = companies_df["industry"] == "Consumer Goods"
    companies_df.loc[consumer_mask, "revenue_growth_rate"] *= 0.5
    companies_df.loc[consumer_mask, "pe_ratio"] *= 0.8

    # Introduce some NaN values
    nan_indices_pe = companies_df.sample(n=5).index
    companies_df.loc[nan_indices_pe, "pe_ratio"] = np.nan

    nan_indices_d2e = companies_df.sample(n=4).index
    companies_df.loc[nan_indices_d2e, "debt_to_equity"] = np.nan

    return companies_df


@wf.spool_asset
@dataclass
class MarketData:
    risk_free_rate: float
    equity_risk_premium: float
    industry_betas: dict[str, float]


@wf.spool_asset(custom_engine={"csv": pd.read_csv})
@dataclass
class AnalystRatings:
    analyst_ratings_registry: dict[str, pd.DataFrame] # name corresponds to filestem


@wf.spool_asset(custom_engine={"csv": pd.read_csv})
@dataclass
class IndustryMetrics:
    industry_metrics_registry: dict[str, pd.DataFrame] # name corresponds to filestem


@wf.spool_asset(file="cap_pe_registry.yaml") # specific file for demonstration purpose, not necessary
@dataclass
class CapPERegistry:
    pe_caps: dict[str, float] # remember: name corresponds to key in the YAML file unless it is a custom engine


@wf.weave(outputs=["rf_rate", "erp", "betas"], params_from=MarketData)
def get_market_data(
    industry: str, 
    risk_free_rate: float, 
    equity_risk_premium: float,
    industry_betas: dict[str, float], 
    ) -> tuple[Any, ...]:

    return (
        risk_free_rate,
        equity_risk_premium,
        industry.map(industry_betas),
    )


@wf.weave(outputs=["analyst_rating", "price_target"], params_from=AnalystRatings)
def get_analyst_ratings(
    ticker: str, 
    analyst_ratings_registry: pd.DataFrame, 
    ) -> tuple[Any, ...]:
    rslt = pd.merge(
        ticker, 
        analyst_ratings_registry, 
        on="ticker", 
        how="left"
        )
    
    # TODO: Make rslt return work, handle pd.DataFrame output as well
    # TODO: if columns from data frame exists in the database, ignore them
    # TODO: It is annyoing for the user to return as tuple
    return rslt["analyst_rating"], rslt["price_target"]


@wf.weave(outputs=["average_pe_ratio", "average_dividend_yield"], params_from=IndustryMetrics)
def get_industry_metrics(
    industry: str, 
    industry_metrics_registry: pd.DataFrame, 
    ) -> tuple[Any, ...]:
    rslt = pd.merge(
        industry, 
        industry_metrics_registry, 
        on="industry", 
        how="left"
        )
    return rslt["average_pe_ratio"], rslt["average_dividend_yield"]


# TODO: Make refine keep track of the number of rows before and after the task
@wf.refine(on_method="process", params_from=CapPERegistry)
class DataPreprocessor:
    def __init__(self, df: pd.DataFrame, pe_caps: dict[str, float]):
        self.df = df
        # The 'pe_caps' are the nested dictionary from the YAML file
        self.pe_caps = pe_caps

    def _remove_missing_values(self):
        """Drops rows with any NaN values."""
        self.df.dropna(subset=['pe_ratio'], inplace=True) # Only drop if pe_ratio is missing

    def _cap_pe_by_industry(self):
        """Filters out companies with P/E ratios above their industry's cap."""
        # Create a Series of P/E caps by mapping the industry column
        pe_caps_series = self.df["industry"].map(self.pe_caps)
        # Filter the DataFrame in a single vectorized operation
        self.df = self.df[self.df["pe_ratio"] <= pe_caps_series]

    def process(self) -> pd.DataFrame:
        """Orchestrates the preprocessing steps."""
        self._remove_missing_values()
        self._cap_pe_by_industry()
        return self.df
        


if __name__ == "__main__":
    df = generate_data(num_companies=100)

    loomer = wf.Loom(df, [get_market_data, get_analyst_ratings, get_industry_metrics, DataPreprocessor])
    loomer.run()

    print(loomer.database.head())
