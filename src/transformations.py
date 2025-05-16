"""Data transformation module for processing DataFrames."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from src.utils.logging import get_logger

logger = get_logger(__name__)


class Transformation:
    """Base class for data transformations."""

    def __init__(self, name: str) -> None:
        """Initialize a transformation.

        Args:
            name: Name of the transformation
        """
        self.name = name

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation to the DataFrame.

        Args:
            df: DataFrame to transform

        Returns:
            Transformed DataFrame
        """
        msg = "Subclasses must implement transform method"
        raise NotImplementedError(msg)


class ColumnTransformation(Transformation):
    """Transformation that applies a function to specific columns."""

    def __init__(
        self,
        name: str,
        columns: list[str],
        func: Callable[[pd.Series], pd.Series],
    ) -> None:
        """Initialize a column transformation.

        Args:
            name: Name of the transformation
            columns: List of columns to transform
            func: Function to apply to each column
        """
        super().__init__(name)
        self.columns = columns
        self.func = func

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation to the specified columns.

        Args:
            df: DataFrame to transform

        Returns:
            Transformed DataFrame
        """
        result = df.copy()

        for col in self.columns:
            if col in result.columns:
                try:
                    logger.debug(f"Applying {self.name} to column {col}")
                    result[col] = self.func(result[col])
                except Exception as e:
                    msg = (
                        f"Error in transformation {self.name} on column {col}: {e!s}",
                    )
                    logger.exception(msg)
            else:
                logger.warning(f"Column {col} not found for transformation {self.name}")

        return result


class RowTransformation(Transformation):
    """Transformation that applies a function to rows."""

    def __init__(
        self,
        name: str,
        func: Callable[[pd.DataFrame], pd.DataFrame],
        filter_func: Callable[[pd.DataFrame], pd.Series] | None = None,
    ) -> None:
        """Initialize a row transformation.

        Args:
            name: Name of the transformation
            func: Function to apply to DataFrame
            filter_func: Optional function to select rows for transformation
        """
        super().__init__(name)
        self.func = func
        self.filter_func = filter_func

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply transformation to rows.

        Args:
            df: DataFrame to transform

        Returns:
            Transformed DataFrame
        """
        result = df.copy()

        try:
            if self.filter_func is not None:
                mask = self.filter_func(result)
                filtered = result[mask]

                if not filtered.empty:
                    logger.debug(
                        f"Applying {self.name} to {len(filtered)} selected rows",
                    )
                    transformed = self.func(filtered)
                    result.loc[mask] = transformed
                else:
                    logger.debug(f"No rows selected for transformation {self.name}")
            else:
                logger.debug(f"Applying {self.name} to all {len(result)} rows")
                result = self.func(result)
        except Exception:
            logger.exception(f"Error in row transformation {self.name}")

        return result


class ComputedColumnTransformation(Transformation):
    """Transformation that adds computed columns."""

    def __init__(
        self,
        name: str,
        column_specs: dict[str, Callable[[pd.DataFrame], pd.Series]],
    ) -> None:
        """Initialize a computed column transformation.

        Args:
            name: Name of the transformation
            column_specs: Dictionary mapping new column names to functions that compute their values
        """
        super().__init__(name)
        self.column_specs = column_specs

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add computed columns to the DataFrame.

        Args:
            df: DataFrame to transform

        Returns:
            DataFrame with added columns
        """
        result = df.copy()

        for col_name, compute_func in self.column_specs.items():
            try:
                logger.debug(f"Computing column {col_name}")
                result[col_name] = compute_func(result)
            except Exception:
                logger.exception(f"Error computing column {col_name}")

        return result


class TransformationPipeline:
    """Pipeline of transformations to be applied in sequence."""

    def __init__(
        self,
        transformations: list[Transformation] | None = None,
    ) -> None:
        """Initialize a transformation pipeline.

        Args:
            transformations: List of transformations to apply in order
        """
        self.transformations = transformations or []

    def add_transformation(self, transformation: Transformation) -> None:
        """Add a transformation to the pipeline.

        Args:
            transformation: Transformation to add
        """
        self.transformations.append(transformation)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations in sequence.

        Args:
            df: DataFrame to transform

        Returns:
            Transformed DataFrame
        """
        result = df.copy()

        for transformation in self.transformations:
            try:
                logger.info(f"Applying transformation: {transformation.name}")
                result = transformation.transform(result)
            except Exception:
                logger.exception(
                    f"Error in transformation {transformation.name}",
                )

        return result


def _transform_df_worker(args: any) -> None:
    """Worker function to transform a DataFrame.

    Args:
        args: Tuple of (path, df, pipeline_factory)

    Returns:
        Tuple of (path, transformed_df or exception)
    """
    path, df, pipeline_factory = args
    try:
        pipeline = pipeline_factory()
        return path, pipeline.transform(df)
    except Exception as e:
        msg = f"Error transforming {path}"
        logger.exception(msg)
        return path, e


def transform_dataframes_concurrently(
    dataframes: dict[str, pd.DataFrame],
    pipeline_factory: Callable[[], TransformationPipeline],
    max_workers: int | None = None,
) -> dict[str, pd.DataFrame | Exception]:
    """Transform multiple DataFrames concurrently using ThreadPoolExecutor.

    Args:
        dataframes: Dictionary mapping file paths to DataFrames
        pipeline_factory: Function that returns a TransformationPipeline
        max_workers: Maximum number of worker threads

    Returns:
        Dictionary mapping file paths to transformed DataFrames or exceptions
    """
    results = {}

    # Filter out exceptions from input
    valid_dataframes = {
        path: df for path, df in dataframes.items() if not isinstance(df, Exception)
    }

    if valid_dataframes:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _transform_df_worker,
                    (path, df, pipeline_factory),
                ): path
                for path, df in valid_dataframes.items()
            }

            for future in as_completed(futures):
                path, result = future.result()
                results[path] = result

    # Include any errors from input

    results.update(
        {
            path: df for path, df in dataframes.items() if isinstance(df, Exception)
        }  # noqa: COM812
    )
    return results
