import pytest
import duckdb
import polars as pl
from catboost import CatBoostRegressor
from src.catboost_trainer import CatBoostTrainer


@pytest.fixture
def setup_test_env():
    # Create a temporary duckdb connection
    conn = duckdb.connect(':memory:')

    # Create a sample DataFrame for testing
    df_excess_returns = pl.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'AMZN'],
        'date': ['2022-01-01', '2022-01-02', '2022-01-03'],
        'excess_return_ln_6m': [0.1, 0.2, 0.3],
        'excess_return_ln_12m': [0.4, 0.5, 0.6],
        'excess_return_ln_24m': [0.7, 0.8, 0.9],
        'excess_return_ln_36m': [1.0, 1.1, 1.2],
        'feature1': [1.0, 2.0, 3.0],
        'feature2': [4.0, 5.0, 6.0],
        'feature3': ['a', 'b', 'c']
    })

    return conn, df_excess_returns

def test_db_train_df(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.db_train_df()
    assert 'ticker' not in trainer.df_preds.columns
    assert 'date' not in trainer.df_preds.columns

def test_df_train_df(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.df_train_df()
    assert 'ticker' not in trainer.df_preds.columns
    assert 'date' not in trainer.df_preds.columns

def test_split_train_test_pools(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    assert trainer.train_pool is not None
    assert trainer.eval_pool is not None
    assert trainer.test_pool is not None
    assert len(trainer.X_test.columns) == len(df_excess_returns.columns) - 2  # Excluding 'ticker' and 'date'

def test_model_init(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.model_init()
    assert isinstance(trainer.model, CatBoostRegressor)
    assert trainer.model.get_param('iterations') == 200
    assert trainer.model.get_param('learning_rate') == 0.15
    assert trainer.model.get_param('depth') == 6
    assert trainer.model.get_param('l2_leaf_reg') == 20
    assert trainer.model.get_param('min_data_in_leaf') == 15
    assert trainer.model.get_param('model_size_reg') == 0.1
    assert trainer.model.get_param('subsample') == 0.8

def test_model_fit(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    trainer.model_init()
    trainer.model_fit()
    assert trainer.test_rmse is not None
    assert trainer.train_timestamp is not None

def test_ticker_preds(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    trainer.model_init()
    trainer.model_fit()
    df_preds = trainer.ticker_preds('AAPL')
    assert 'predicted_excess_return' in df_preds.columns
    assert 'predicted_std' in df_preds.columns
    assert 'actual_excess_return' in df_preds.columns

def test_feature_importance_plot(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    trainer.model_init()
    trainer.model_fit()
    trainer.feature_importance_plot()
    # No explicit assertion, but the test passes if the plot is generated without errors

def test_get_shap_values(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    trainer.model_init()
    trainer.model_fit()
    trainer.get_shap_values()
    assert len(trainer.shap_values) > 0

def test_shap_beeswarm(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    trainer.model_init()
    trainer.model_fit()
    trainer.shap_beeswarm()
    # No explicit assertion, but the test passes if the plot is generated without errors

def test_all_ticker_shaps(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    trainer.model_init()
    trainer.model_fit()
    df_shaps = trainer.all_ticker_shaps()
    assert 'date' in df_shaps.columns
    assert 'ticker' in df_shaps.columns
    assert 'feature' in df_shaps.columns
    assert 'shap_value' in df_shaps.columns
    assert 'feature_value' in df_shaps.columns
    assert 'bias' in df_shaps.columns
    assert 'predicted_value_log' in df_shaps.columns
    assert 'actual_value_log' in df_shaps.columns

def test_ticker_shap(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.split_train_test_pools()
    trainer.model_init()
    trainer.model_fit()
    df_shaps = trainer.ticker_shap('AAPL')
    assert 'Date' in df_shaps.columns
    assert 'Ticker' in df_shaps.columns
    assert 'Feature' in df_shaps.columns
    assert 'SHAP Value' in df_shaps.columns
    assert 'Feature Value' in df_shaps.columns
    assert 'Bias' in df_shaps.columns
    assert 'Predicted Value (log)' in df_shaps.columns
    assert 'Predicted Value' in df_shaps.columns
    assert 'Predicted Std' in df_shaps.columns
    assert 'Actual Value (log)' in df_shaps.columns
    assert 'Actual Value' in df_shaps.columns

if __name__ == '__main__':
    pytest.main([__file__])
