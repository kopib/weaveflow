<div align="center">
  <img src="https://raw.githubusercontent.com/kopi-cloud/weaveflow/main/assets/logo/weaveflow_logo.png" alt="weaveflow logo" width="200"/>
  <h1>weaveflow</h1>
  <p>
    <strong>Make your pandas processes clearer, your functions <code>weaveable</code>, and your data processing <code>refineable</code>. Create beautiful, standardized, and visual data pipelines.</strong>
  </p>
  <p>
    <a href="https://pypi.org/project/weaveflow/"><img alt="PyPI" src="https://img.shields.io/pypi/v/weaveflow.svg"></a>
    <a href="https://github.com/kopi-cloud/weaveflow/actions"><img alt="CI" src="https://github.com/kopi-cloud/weaveflow/workflows/CI/badge.svg"></a>
    <a href="https://codecov.io/gh/kopi-cloud/weaveflow"><img alt="Codecov" src="https://codecov.io/gh/kopi-cloud/weaveflow/branch/main/graph/badge.svg"></a>
    <a href="https://opensource.org/licenses/MIT"><img alt="License" src="https://img.shields.io/badge/License-MIT-yellow.svg"></a>
  </p>
</div>

---

**weaveflow** is a Python library designed to bring clarity, structure, and visibility to your pandas data processing workflows. It transforms complex sequences of operations into a declarative, dependency-aware pipeline that is easy to read, maintain, and visualize.

Stop wrestling with tangled scripts and start weaving elegant data stories.

## ✨ Core Concepts

`weaveflow` introduces a few simple but powerful concepts to structure your data pipelines:

*   **🧵 Weaving**: Make your functions **`weaveable`**. A `@weave` decorator turns any Python function that operates on pandas Series into a node in a dependency graph. It automatically tracks inputs (from DataFrame columns) and outputs (to new DataFrame columns), building a clear feature engineering lineage.

*   **🔪 Refining**: Make your data **`refineable`**. A `@refine` decorator marks classes or functions that perform larger, sequential transformations on the entire DataFrame, such as cleaning, filtering, dropping rows, or grouping. These steps form a clear, linear processing chain.

*   ** spooling**: Externalize your parameters effortlessly. The `@spool_asset` decorator loads constants, configurations, and even small data files (like CSVs or YAML) into dataclasses, making your pipeline's parameters transparent and easy to manage outside your code.

*   **🧶 Loom**: The `Loom` is the heart of `weaveflow`. It's the orchestrator that takes your initial DataFrame and a list of `weaveable` and `refineable` tasks, and executes them in the correct order, managing all dependencies automatically.

*   **📊 Visualization**: `weaveflow` automatically generates intuitive graphs of your pipeline.
    *   The **`WeaveGraph`** shows the dependency network of your feature engineering (`@weave`) steps.
    *   The **`RefineGraph`** shows the sequential flow of your data refinement (`@refine`) steps.

## 🚀 Key Features

*   **Declarative Pipelines**: Define *what* you want to do, not *how*. `weaveflow` handles the execution order.
*   **Automatic Dependency Graph**: Understand at a glance how your features are derived. No more guessing which function created which column.
*   **Clear Separation of Concerns**: A clean distinction between column-wise feature creation (`@weave`) and table-wise transformations (`@refine`).
*   **Effortless Parameterization**: Decouple configuration from logic using `@spool_asset` with YAML, JSON, TOML, and even custom file types.
*   **Stunning Visualizations**: Generate `graphviz` diagrams of your entire workflow to share with your team, document your process, or debug complex flows.
*   **Reproducibility**: By structuring your code and externalizing parameters, `weaveflow` pipelines are easier to reproduce and validate.
*   **Code as Configuration**: Your pipeline is defined by a simple list of functions and classes, making it self-documenting.

## 📦 Installation

You can install `weaveflow` via `pip`:

```bash
pip install weaveflow
```

You will also need to install `graphviz` to render the pipeline graphs.

```bash
# For Debian/Ubuntu
sudo apt-get install graphviz

# For MacOS (using Homebrew)
brew install graphviz
```

## 🏁 Quickstart: A Financial Analysis Pipeline

Let's build a pipeline to identify undervalued stocks from a dataset of companies.

### 1. Define Your Data Sources (`@spool_asset`)

First, let's externalize our market assumptions into a YAML file. This keeps our code clean and parameters easy to change.

`assets/data/market_data.yaml`:
```yaml
risk_free_rate: 0.02
equity_risk_premium: 0.055
industry_betas:
  Technology: 1.2
  Healthcare: 0.9
  Industrials: 1.0
  Consumer Goods: 0.8
```

Now, create a "spool asset" to load this data into our pipeline.

```python
from dataclasses import dataclass
import weaveflow as wf

@wf.spool_asset
@dataclass
class MarketData:
    risk_free_rate: float
    equity_risk_premium: float
    industry_betas: dict[str, float]
```

### 2. Make Your Functions `weaveable`

Next, define functions to calculate new columns. The `@weave` decorator declares the output columns and automatically injects required columns (like `industry`) and spooled parameters (from `MarketData`).

```python
import pandas as pd

@wf.weave(outputs=["rf_rate", "erp", "betas"], params_from=MarketData)
def get_market_data(
    industry: pd.Series,
    risk_free_rate: float,
    equity_risk_premium: float,
    industry_betas: dict[str, float],
) -> tuple:
    return (
        risk_free_rate,
        equity_risk_premium,
        industry.map(industry_betas),
    )

@wf.weave("cost_of_equity")
def calculate_cost_of_equity(rf_rate: float, erp: float, betas: float) -> float:
    """Calculates Cost of Equity using the CAPM model."""
    return rf_rate + betas * erp
```

### 3. Make Your Processing Steps `refineable`

Use a `@refine` class to perform multi-step data cleaning and filtering on the entire DataFrame. `weaveflow` will call the `process` method.

```python
@wf.refine(on_method="process")
class DataPreprocessor:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def _remove_missing_values(self):
        self.df.dropna(subset=["pe_ratio"], inplace=True)

    def process(self) -> pd.DataFrame:
        """Orchestrates the preprocessing steps."""
        self._remove_missing_values()
        # ... other cleaning steps ...
        return self.df
```

### 4. Weave it all together with `Loom`

The `Loom` class is the orchestrator. You provide it with your initial data and a list of all your `weave` and `refine` tasks. `weaveflow` handles the rest.

```python
# main.py
import pandas as pd
import weaveflow as wf
from your_module import get_market_data, calculate_cost_of_equity, DataPreprocessor #, ... other tasks

# 1. Generate or load your initial DataFrame
df = generate_data(num_companies=1000)

# 2. Define the pipeline tasks in order
pipeline_tasks = [
    get_market_data,
    DataPreprocessor,
    calculate_cost_of_equity,
    # ... add all your other weave and refine tasks here
]

# 3. Create and run the Loom
loomer = wf.Loom(
    df,
    tasks=pipeline_tasks,
    weaveflow_name="UnderValuedCompanies"
)
loomer.run()

# The final, processed DataFrame is in loomer.database
print(loomer.database.head())
```

### 5. Visualize Your Flow

Now for the magic. Let's visualize the pipeline we just created.

```python
# Visualize the feature engineering dependencies
weave_graph = wf.WeaveGraph(loomer)
weave_graph.build().render("assets/output/weave_graph", view=True)

# Visualize the sequential refinement steps
refine_graph = wf.RefineGraph(loomer)
refine_graph.build().render("assets/output/refine_graph", view=True)
```

This generates two beautiful graphs:

#### Weave Graph
*Shows how your columns are created and what they depend on.*

!Weave Graph Example

#### Refine Graph
*Shows the high-level, sequential stages of your data transformation.*

!Refine Graph Example

## Contributing

Contributions are welcome! Whether it's bug reports, feature requests, or code contributions, please feel free to open an issue or a pull request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.