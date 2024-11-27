
import duckdb
import pandas as pd
import numpy as np
from typing import Set
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from catboost import CatBoostRegressor, Pool
import matplotlib.pyplot as plt
import shap

class PrepData:
    """Prepares data from DuckDB for training."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, pred_col: str, seed: int) -> None:
        """Init with DuckDB connection."""
        self.conn: duckdb.DuckDBPyConnection = conn
        self.pred_col: str = pred_col
        self.seed: int = seed
        self._all_exclude_cols: Set[str] = {
            'date', 'name',
            'excess_return_ln_6m', 'excess_return_ln_12m',
            'excess_return_ln_24m', 'excess_return_ln_36m',
        }
        self._model_exclude_cols: Set[str] = self._all_exclude_cols | {'ticker'}
        self._na_fill_value: int = -999999999
        self.categorical_feature_indices: np.ndarray
        self.df_preds: pd.DataFrame
        self.df_excess_returns: pd.DataFrame
        self.X_test: pd.DataFrame
        self.X_test_full: pd.DataFrame
        self.y_test: pd.Series
        self.test_tickers: pd.Series
        self.train_pool: Pool
        self.eval_pool: Pool
        self.test_pool: Pool
        self.model: CatBoostRegressor
        self.shap_values: np.ndarray = np.array([])

    # TODO: add some light dataframe that provides the missing link between df_excess_returns and df_preds
    # which should be used when joining prediction results to actual data.

    def db_excess_returns(self) -> None:
        """Get full DataFrame from DuckDB."""
        self.df_excess_returns = self.conn.query("""
            select * from fundamentals.excess_returns
        """).df()

    def db_train_df(self) -> None:
        """Get training DataFrame excluding specified cols except prediction col.

        Returns: pd.DataFrame: Training data with specified cols excluded
        """
        exclude_cols = self._all_exclude_cols.difference({self.pred_col})
        print(exclude_cols)
        exclude_cols_str = ',\n'.join(exclude_cols)

        self.df_preds = self.conn.query(f"""
            select * exclude({exclude_cols_str})
            from fundamentals.excess_returns
            where {self.pred_col} is not null
        """).df()
        self.df_preds = self.df_preds.fillna(self._na_fill_value)

        size_bytes = self.df_preds.memory_usage(deep=True).sum()
        size_mb = size_bytes / (1024 * 1024)
        print(f"DataFrame size: {size_mb:.2f} MB")

    def df_train_df(self) -> None:
        """Get training DataFrame from existing DataFrame.

        Returns: pd.DataFrame: Training data subset
        """
        if self.df_excess_returns is None:
            raise ValueError("No data loaded. Run db_excess_returns() first.")

        exclude_cols = self._all_exclude_cols.difference({self.pred_col})
        print(exclude_cols)
        self.df_preds = self.df_excess_returns.drop(columns=list(exclude_cols))
        self.df_preds = self.df_preds.dropna(subset=[self.pred_col])
        self.df_preds = self.df_preds.fillna(self._na_fill_value)

        size_bytes = self.df_preds.memory_usage(deep=True).sum()
        size_mb = size_bytes / (1024 * 1024)
        print(f"DataFrame size: {size_mb:.2f} MB")

    def split_train_test_pools(self, test_size = 0.05, val_size = 0.1) -> None:
        # First split the full dataset including ticker
        X_temp_full, X_test_full, y_temp, self.y_test = train_test_split(
            self.df_preds.drop(self.pred_col, axis=1),  # Keep ticker here
            self.df_preds[self.pred_col],
            test_size=test_size,
            random_state=self.seed
        )

        # Store X_test with ticker for later use
        self.X_test_full = X_test_full
        self.X_test = X_test_full.drop('ticker', axis=1)

        # Create version without ticker for model training
        X_temp = X_temp_full.drop('ticker', axis=1)

        # Get categorical features indices after ticker is removed
        self.categorical_features_indices = np.where(self.X_test.dtypes != float)[0]

        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp, train_size=(1-val_size), random_state=self.seed
        )

        self.train_pool = Pool(X_train, y_train, cat_features=self.categorical_features_indices)
        self.eval_pool = Pool(X_val, y_val, cat_features=self.categorical_features_indices)
        self.test_pool = Pool(self.X_test, self.y_test, cat_features=self.categorical_features_indices)


    def model_init(self) -> None:
        # TODO maybe: include model for excess_return_ln_6m, even though it overfits
        if self.pred_col == 'excess_return_ln_6m':
            # To mitigate overfitting
            model_size_reg = 0.1
            l2_leaf_reg = 20
            subsample = 0.8
            min_data_in_leaf=15
        else:
            l2_leaf_reg = 15
            model_size_reg = 0.08
            subsample = 0.9
            min_data_in_leaf=10

        depth=6
        iterations = 200 # TODO: increase to 2000
        learning_rate = 0.15
        early_stopping_rounds = 25

        self.model = CatBoostRegressor(
            iterations=iterations,
            learning_rate=learning_rate,
            depth=depth,
            l2_leaf_reg=l2_leaf_reg,
            min_data_in_leaf=min_data_in_leaf,
            model_size_reg=model_size_reg,
            subsample=subsample,
            loss_function='RMSEWithUncertainty',
            early_stopping_rounds=early_stopping_rounds,
            verbose=False,
            random_seed=self.seed,
        )


    def model_fit(self) -> None:
        self.model.fit(
            self.train_pool,
            eval_set=self.eval_pool,
            use_best_model=True,
            verbose=100,
        )

        # Get test set predictions
        test_preds = self.model.predict(self.test_pool)
        mean_preds = test_preds[:, 0]  # Get only mean predictions, not variance
        test_rmse = np.sqrt(mean_squared_error(self.test_pool.get_label(), mean_preds))

        print("\nOutcome variable:", self.pred_col)
        print("Best iteration:", self.model.get_best_iteration())
        print("Validation RMSE:", self.model.get_best_score()['validation']['RMSEWithUncertainty'])
        print(f"Holdout test set RMSE: {test_rmse:.6f}")

    def ticker_preds(self, ticker: str, since: str = "2019-01-01") -> pd.DataFrame:
        """Get predictions for a single ticker."""
        ticker = ticker.upper()
        exclude_cols = ", ".join(self._model_exclude_cols)

        df_preds = self.conn.query(f"""
            select * exclude({exclude_cols})
            from fundamentals.excess_returns
            where ticker = '{ticker}' and date > '{since}'
        """).df().fillna(self._na_fill_value)

        df_actual = self.conn.query(f"""
            select date, {self.pred_col}
            from fundamentals.excess_returns
            where ticker = '{ticker}' and date > '{since}'
        """).df()

        preds = pd.DataFrame(self.model.predict(df_preds), columns=['mean', 'var'])
        df_out = (
            pd.concat([preds, df_preds.reset_index(drop=True), df_actual.reset_index(drop=True)], axis=1)
            .assign(
                predicted_excess_return=lambda df: np.exp(df['mean']),
                predicted_std=lambda df: np.sqrt(np.exp(2 * df['mean']) * df['var']),
                actual_excess_return=lambda df: np.exp(df[self.pred_col])
            )
            .filter(['predicted_excess_return', 'predicted_std', 'ticker', 'date', 'actual_excess_return'])
            .sort_values('date', ascending=False)
        )
        return df_out

    def feature_importance_plot(self) -> None:
        # TODO: consider making this plot denser in y-axis (smaller bars)
        feature_imp_df = pd.DataFrame({
            'feature': self.X_test.columns,
            'importance': self.model.feature_importances_
        })

        # Sort by importance descending
        feature_imp_df = feature_imp_df.sort_values('importance', ascending=True)

        # Plot horizontal bar chart
        plt.figure(figsize=(10, max(8, len(feature_imp_df) * 0.3)))
        plt.barh(feature_imp_df['feature'], feature_imp_df['importance'])
        plt.title('Feature Importances')
        plt.xlabel('Importance')
        plt.tight_layout()
        plt.show()

    def get_shap_values(self) -> None:
        shap_values = self.model.get_feature_importance(
            data=self.test_pool,
            type='ShapValues',
            shap_mode='UsePreCalc',
            verbose=False
        )

        # Remove variance column
        if len(shap_values.shape) == 3:
            shap_values = shap_values[:, 0, :]

        self.feature_names = self.X_test.columns
        self.shap_values = shap_values

    def shap_beeswarm(self) -> None:
        # TODO: is it possible to return the plot instead of showing it?
        # Might not be necessary, creating the plot takes 1.7 seconds on macbook
        if len(self.shap_values) == 0:
            self.get_shap_values()

        # Remove the bias term (last column)
        shap_values = self.shap_values[:, :-1]

        # Print shapes for debugging
        print("SHAP values shape:", shap_values.shape)
        print("X_eval shape:", self.X_test.values.shape)
        print("Number of feature names:", len(self.feature_names))

        explanation = shap.Explanation(
            values=shap_values,
            base_values=np.zeros(len(self.X_test)),
            data=self.X_test.values,
            feature_names=self.feature_names
        )

        shap.plots.beeswarm(explanation, max_display=40)

    def ticker_shap(self, ticker: str) -> pd.DataFrame:
        ticker = ticker.upper()

        # Create a Pool for just this ticker's data
        ticker_data = self.df_preds[self.df_preds['ticker'] == ticker].copy()
        if len(ticker_data) == 0:
            raise ValueError(f"Ticker {ticker} not found in dataset")

        X_ticker = ticker_data.drop(['ticker', self.pred_col], axis=1)
        y_ticker = ticker_data[self.pred_col]

        ticker_pool = Pool(X_ticker, y_ticker, cat_features=self.categorical_features_indices)

        # Calculate SHAP values for this specific ticker
        shap_values = self.model.get_feature_importance(
            data=ticker_pool,
            type='ShapValues',
            shap_mode='UsePreCalc',
            verbose=False
        )

        # Remove variance column if present
        if len(shap_values.shape) == 3:
            shap_values = shap_values[:, 0, :]

        # Create DataFrame with results
        row_data = pd.DataFrame({
            'Feature': self.X_test.columns,
            'SHAP Value': shap_values[0, :-1],  # Take first (and only) row
            'Feature Value': X_ticker.iloc[0].values
        })

        # Sort by absolute SHAP values
        row_data['Abs SHAP'] = row_data['SHAP Value'].abs()
        row_data_sorted = row_data.sort_values('Abs SHAP', ascending=False)
        row_data_sorted = row_data_sorted.drop('Abs SHAP', axis=1)

        print(f"Bias (expected value): {shap_values[0, -1]:0.4f}")
        print(f"Actual target value (log scale): {y_ticker.iloc[0]:0.4f}")
        print(f"Actual target value (original scale): {np.exp(y_ticker.iloc[0]):0.4f}\n")

        return row_data_sorted


# TODO
# - Final artifact should all 4 models and the df_excess_returns (but it can get that one from motherduck)
# - Add method that deletes all dataframes from the class, prior to saving the model
# - Use joblib to save the model artifact
# - Consider pre-computing shap values etc. in the artifact, instead of doing this compute in streamlit
#   - yes, makes sense, then I don't need the dataframe in the artifact at all, if all model assessment metrics are already computed


