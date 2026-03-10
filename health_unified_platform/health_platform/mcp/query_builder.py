"""Query builder that reads YAML semantic contracts and builds parameterized SQL.

Loads metric contracts from the contracts/metrics/ directory, caches them,
and substitutes parameters into SQL templates.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from health_platform.utils.logging_config import get_logger

logger = get_logger("query_builder")


def _convert_dates(data: object) -> object:
    """Recursively convert datetime.date values to ISO strings.

    PyYAML parses YYYY-MM-DD as datetime.date objects. This converts
    them back to strings so they can be used safely in comparisons
    and substitutions.
    """
    import datetime

    if isinstance(data, dict):
        return {k: _convert_dates(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_convert_dates(item) for item in data]
    elif isinstance(data, datetime.date) and not isinstance(data, datetime.datetime):
        return data.isoformat()
    return data


class QueryBuilder:
    """Reads YAML contracts and builds parameterized SQL queries.

    Parameters
    ----------
    contracts_dir : Path
        Path to the contracts/metrics/ directory containing YAML files.
    """

    def __init__(self, contracts_dir: Path) -> None:
        self.contracts_dir = contracts_dir
        self._cache: dict[str, dict] = {}

    def load_contract(self, metric_name: str) -> dict:
        """Load and cache a YAML contract for a metric.

        Parameters
        ----------
        metric_name : str
            Name of the metric (matches filename without .yml extension).

        Returns
        -------
        dict
            Parsed YAML contract.

        Raises
        ------
        FileNotFoundError
            If no contract file exists for the given metric name.
        """
        if metric_name in self._cache:
            return self._cache[metric_name]

        contract_path = self.contracts_dir / f"{metric_name}.yml"
        if not contract_path.exists():
            raise FileNotFoundError(
                f"No contract found for metric '{metric_name}' at {contract_path}"
            )

        with open(contract_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        data = _convert_dates(data)
        self._cache[metric_name] = data
        logger.debug("Loaded contract for metric '%s'", metric_name)
        return data

    def build_query(self, metric_name: str, computation: str, params: dict) -> str:
        """Build a complete SQL query from a metric contract.

        Parameters
        ----------
        metric_name : str
            Name of the metric.
        computation : str
            Computation type (e.g., 'daily_value', 'period_average', 'trend').
        params : dict
            Parameters to substitute (e.g., {'date': '2026-03-01', 'start': '...', 'end': '...'}).

        Returns
        -------
        str
            Ready-to-execute SQL query with parameters substituted.

        Raises
        ------
        FileNotFoundError
            If the metric contract does not exist.
        ValueError
            If the computation type is not defined in the contract.
        """
        sql_template = self.get_computation_sql(metric_name, computation)
        return self.substitute_params(sql_template, params)

    def get_index(self) -> dict:
        """Load the _index.yml file (schema pruning index).

        Returns
        -------
        dict
            Parsed index data with metrics, query_routing, and output_format.
        """
        index_path = self.contracts_dir / "_index.yml"
        if not index_path.exists():
            logger.warning("_index.yml not found at %s", index_path)
            return {}

        with open(index_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return _convert_dates(data) if data else {}

    def get_business_rules(self) -> dict:
        """Load the _business_rules.yml file.

        Returns
        -------
        dict
            Parsed business rules with composite scores, alerts, and anomaly detection.
        """
        rules_path = self.contracts_dir / "_business_rules.yml"
        if not rules_path.exists():
            logger.warning("_business_rules.yml not found at %s", rules_path)
            return {}

        with open(rules_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return _convert_dates(data) if data else {}

    def list_metrics(self) -> list[str]:
        """List all available metric YAML files (excluding index and business rules).

        Returns
        -------
        list[str]
            Sorted list of metric names (without .yml extension).
        """
        if not self.contracts_dir.exists():
            return []

        metrics = []
        for path in self.contracts_dir.glob("*.yml"):
            name = path.stem
            # Skip internal files
            if name.startswith("_"):
                continue
            metrics.append(name)

        return sorted(metrics)

    def get_computation_sql(self, metric_name: str, computation: str) -> str:
        """Return the raw SQL template for a specific computation.

        Parameters
        ----------
        metric_name : str
            Name of the metric.
        computation : str
            Computation type.

        Returns
        -------
        str
            Raw SQL template with :param placeholders.

        Raises
        ------
        FileNotFoundError
            If the metric contract does not exist.
        ValueError
            If the computation type is not defined in the contract.
        """
        contract = self.load_contract(metric_name)
        metric_def = contract.get("metric", contract)
        computations = metric_def.get("computations", {})

        if computation not in computations:
            available = list(computations.keys())
            raise ValueError(
                f"Computation '{computation}' not found for metric '{metric_name}'. "
                f"Available: {', '.join(available)}"
            )

        sql = computations[computation].get("sql", "")
        if not sql:
            raise ValueError(
                f"No SQL defined for computation '{computation}' "
                f"on metric '{metric_name}'."
            )

        return sql.strip()

    @staticmethod
    def substitute_params(sql: str, params: dict) -> str:
        """Replace :param placeholders with SQL date literals.

        Substitutes :date, :start, :end (and any other keys in params)
        with properly quoted SQL values like '2026-03-01'.

        Parameters
        ----------
        sql : str
            SQL template with :param placeholders.
        params : dict
            Parameter name -> value mapping.

        Returns
        -------
        str
            SQL with all parameters substituted.
        """
        result = sql
        # Sort by key length descending to avoid partial replacements
        # (e.g., :start_date before :start)
        for key in sorted(params.keys(), key=len, reverse=True):
            value = params[key]
            placeholder = f":{key}"
            # Quote string values as SQL literals
            if isinstance(value, str):
                safe_value = f"'{value}'"
            elif value is None:
                safe_value = "NULL"
            else:
                safe_value = str(value)
            result = result.replace(placeholder, safe_value)

        return result
