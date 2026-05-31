# Databricks notebook source
# ML training template: UC table -> train -> MLflow log -> predictions table.
#
# Replace the training code in the EDIT THIS section. Everything else
# is reusable orchestration: data load, train/test split, MLflow
# logging, predictions write.

# COMMAND ----------

# === Configuration ========================================================

TRAINING_TABLE = None        # set via widget, e.g. "analytics.ml.customer_features"
PREDICTIONS_TABLE = None     # set via widget; empty = skip predictions write
EXPERIMENT_NAME = None       # set via widget, e.g. "/Shared/experiments/churn-model"
TARGET_COLUMN = "target"     # name of the target/label column in TRAINING_TABLE
TEST_SIZE = 0.2
RANDOM_STATE = 42
MAX_ROWS = None              # smoke test: limit rows for fast iteration

try: TRAINING_TABLE = dbutils.widgets.get("training_table") or TRAINING_TABLE
except Exception: pass
try: PREDICTIONS_TABLE = dbutils.widgets.get("predictions_table") or PREDICTIONS_TABLE
except Exception: pass
try: EXPERIMENT_NAME = dbutils.widgets.get("experiment_name") or EXPERIMENT_NAME
except Exception: pass
try: TARGET_COLUMN = dbutils.widgets.get("target_column") or TARGET_COLUMN
except Exception: pass
try:
    _mr = dbutils.widgets.get("max_rows")
    MAX_ROWS = int(_mr) if _mr else None
except Exception: pass

assert TRAINING_TABLE, "training_table must be set"
assert EXPERIMENT_NAME, "experiment_name must be set"

# COMMAND ----------

# === Load + split data ====================================================

import mlflow
mlflow.autolog()
mlflow.set_experiment(EXPERIMENT_NAME)

train_df = spark.table(TRAINING_TABLE)
if MAX_ROWS is not None and MAX_ROWS > 0:
    train_df = train_df.limit(MAX_ROWS)

print(f"Loaded {train_df.count()} rows from {TRAINING_TABLE}")

# Convert to Pandas for sklearn. For large training sets, switch to
# Spark MLlib or distributed training; the orchestration story is the
# same.
pdf = train_df.toPandas()
y = pdf[TARGET_COLUMN]
X = pdf.drop(columns=[TARGET_COLUMN])

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
)

# COMMAND ----------

# === EDIT THIS: train your model ==========================================
# Placeholder: linear regression. Replace with your actual model and
# hyperparameter tuning. mlflow.autolog() above captures params, metrics,
# and the fitted model automatically for most popular libraries.

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

with mlflow.start_run() as run:
    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    r2 = r2_score(y_test, y_pred)

    mlflow.log_metric("rmse_test", rmse)
    mlflow.log_metric("r2_test", r2)

    print(f"Run {run.info.run_id}: rmse={rmse:.4f}, r2={r2:.4f}")
    run_id = run.info.run_id

# === END EDIT THIS ========================================================

# COMMAND ----------

# === Optional: write predictions to UC ====================================

if PREDICTIONS_TABLE:
    import pandas as pd
    predictions_pdf = pd.DataFrame({
        "predicted": y_pred,
        "actual": y_test.values,
        "run_id": run_id,
    })
    pred_df = spark.createDataFrame(predictions_pdf)
    target_catalog, target_schema, target_table = PREDICTIONS_TABLE.split(".")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
    pred_df.write.mode("overwrite").saveAsTable(PREDICTIONS_TABLE)
    print(f"Wrote {pred_df.count()} predictions to {PREDICTIONS_TABLE}")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "experiment": EXPERIMENT_NAME,
    "rmse": float(rmse),
    "r2": float(r2),
    "predictions_table": PREDICTIONS_TABLE,
}))
