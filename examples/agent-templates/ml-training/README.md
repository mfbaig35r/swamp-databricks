# Template: ML training (UC → notebook → MLflow)

Read training data from a UC table, train a model in a Databricks
notebook, log the run + model + metrics to MLflow, optionally register
to UC Model Registry.

**Use when**: you have training data in a UC table and want to train a
model on it on a scheduled cadence (nightly retrain, weekly refresh).

**Don't use this for**: distributed training requiring multiple GPUs
(Databricks has dedicated solutions), real‑time inference (use a
serving endpoint, future swamp-databricks model), or feature engineering
pipelines (use DLT or a dedicated transform job).

## What this template does

1. Ensures the target UC schema (for predictions output) exists
2. Uploads a training notebook with placeholder scikit‑learn code
3. Defines an idempotent job that runs the notebook
4. Triggers and waits
5. Optionally reads the resulting MLflow run summary

The notebook is a placeholder. Replace its body with your actual
training code. MLflow autolog is enabled, so most popular libraries
(scikit‑learn, XGBoost, PyTorch, TensorFlow, Spark MLlib) will log
automatically without code changes.

## Why no MLflow model in the pack

swamp‑databricks v0.14 doesn't have models for MLflow experiments,
runs, registered models, or model serving endpoints yet. They're in
the roadmap. For now, the notebook handles MLflow interactions via the
mlflow Python library directly. When the MLflow models ship in a
future version, this template will be updated to use them.

## Customize before running

In `workflow.yaml`:

- `inputs.training_table`: e.g. `analytics.ml.customer_features`
- `inputs.predictions_table`: where predictions land (optional)
- `inputs.experiment_name`: MLflow experiment path
- `schedule:` field: cron expression for production retrains

In the notebook content (inlined in `workflow.yaml`):

- The training code itself: model class, hyperparameters, features
- Train/test split strategy
- Evaluation metrics to log
- Whether to register the model to UC Model Registry

## Auth setup

MLflow on Databricks is auto‑configured. No credentials needed.

If your training data is in a UC schema you don't already have
permissions on, add a `uc_permissions.update` step before the job to
grant `SELECT` to your user or service principal.

## Standalone notebook source

[`notebook.py`](./notebook.py) on GitHub. Inlined in `workflow.yaml`.

## Smoke test before scheduling

Run with `max_rows=1000` (template reads only that many training rows):

```sh
swamp workflow run workflow.yaml --input max_rows=1000
```

A 1000‑row train of the placeholder linear regression completes in
seconds. Use this to validate UC read access, the MLflow logging
chain, and the workflow shape before scaling to real training.

Production: drop `max_rows`, set actual training table, add
`schedule:` for nightly/weekly retrains.

## Roadmap

When the swamp‑databricks pack ships MLflow models (experiment,
registered_model, model_version, model_serving_endpoint), this
template will be updated to:

- Use `experiment.create_or_update` instead of relying on the
  notebook to ensure the experiment exists
- Use `registered_model.create_or_update` for the UC Model Registry
  side
- Optionally chain a `model_serving_endpoint.deploy` step for real‑time
  inference

For now, the notebook handles all of that directly.
