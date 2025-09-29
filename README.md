# 📊 Portfolio Optimization and Risk Management using Machine Learning

## 🔎 Project Overview

This project applies machine learning approaches to portfolio optimization and risk management. It combines Hierarchical Risk Parity (HRP) for diversification and Reinforcement Learning (RL) for dynamic allocation, providing a modern alternative to classical portfolio theory.

The project demonstrates how data science, statistics, and ML can be applied to solve practical problems in asset management, such as:

* Constructing risk-balanced portfolios.

* Adapting to changing market regimes.

* Simulating allocation policies under different risk constraints.

## 🏗️ Key Features

* Data Pipeline: Automated download, cleaning, and storage of financial time series (stocks, ETFs, crypto).

* Return & Risk Metrics: Computation of daily returns, volatility, covariance matrices, Sharpe ratios.

* Hierarchical Risk Parity (HRP):
    *  Asset clustering using hierarchical clustering (correlation distance).

    * Recursive bisection for weight allocation.

    * Outperforms naive equal-weight and classical mean-variance optimization in stability.

* Reinforcement Learning (RL):

    * Custom PortfolioEnv (Gym-style environment).

    * Agent learns dynamic allocation based on returns, risk signals, and transaction costs.

    * Comparison of RL-based strategies vs. HRP.

* Backtesting Framework:

    * Walk-forward testing.

    * Performance metrics: cumulative returns, drawdowns, volatility, max drawdown, Sharpe ratio.

    * Visualization of portfolio weights over time.