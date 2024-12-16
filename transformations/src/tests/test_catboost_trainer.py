import pytest
import duckdb
import polars as pl
from catboost import CatBoostRegressor
from src.catboost_trainer import CatBoostTrainer
import os


@pytest.fixture(autouse=True)
def set_app_env():
    os.environ['APP_ENV'] = 'test'
    yield
    del os.environ['APP_ENV']


@pytest.fixture
def setup_test_env():
    # Create a temporary duckdb connection
    conn = duckdb.connect(':memory:')

    # Create a sample DataFrame for testing
    df_excess_returns = pl.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'AMZN'],
        'name': ['Apple Inc.', 'Microsoft Corporation', 'Amazon.com Inc.'],
        'date': ['2022-01-01', '2022-01-02', '2022-01-03'],
        'excess_return_ln_6m': [0.1, 0.2, 0.3],
        'excess_return_ln_12m': [0.4, 0.5, 0.6],
        'excess_return_ln_24m': [0.7, 0.8, 0.9],
        'excess_return_ln_36m': [1.0, 1.1, 1.2],
        'feature1': [1.0, 2.0, 3.0],
        'feature2': [4.0, 5.0, 6.0],
        'feature3': [4.0, 5.0, 6.0],
    })

    return conn, df_excess_returns


def test_df_train_df(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_12m', 42)
    trainer.df_train_df()
    assert 'excess_return_ln_6m' not in trainer.df_preds.columns
    assert 'excess_return_ln_24m' not in trainer.df_preds.columns
    assert 'excess_return_ln_36m' not in trainer.df_preds.columns
    assert 'name' not in trainer.df_preds.columns

def test_split_train_test_pools(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.df_train_df()
    trainer.split_train_test_pools()
    assert trainer.train_pool is not None
    assert trainer.eval_pool is not None
    assert trainer.test_pool is not None
    # Compare with all columns starting with 'feature'
    assert len(trainer.X_test.columns) == len(df_excess_returns.select(pl.col('^feature.*$')).columns)

def test_model_init(setup_test_env):
    conn, df_excess_returns = setup_test_env
    trainer = CatBoostTrainer(conn, df_excess_returns, 'excess_return_ln_6m', 42)
    trainer.df_train_df()
    trainer.split_train_test_pools()
    trainer.model_init()
    assert isinstance(trainer.model, CatBoostRegressor)
    assert trainer.model.get_param('iterations') == 1
    assert trainer.model.get_param('learning_rate') == 0.15
    assert trainer.model.get_param('depth') == 6
    assert trainer.model.get_param('l2_leaf_reg') == 20
    assert trainer.model.get_param('min_data_in_leaf') == 15
    assert trainer.model.get_param('model_size_reg') == 0.1
    assert trainer.model.get_param('subsample') == 0.8

if __name__ == '__main__':
    pytest.main([__file__])
