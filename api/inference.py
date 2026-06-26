import pandas as pd

from api.model_loader import get_models

RF_FEATURES = [
    "magnitude", "depth_km", "quake_count_24h", "avg_magnitude_24h",
    "max_magnitude_24h", "shallow_quake_ratio_24h", "felt_count_24h",
    "swarm_density", "latitude", "longitude",
]

ISO_FEATURES = [
    "magnitude", "depth_km", "quake_count_24h", "avg_magnitude_24h",
    "max_magnitude_24h", "shallow_quake_ratio_24h",
    "latitude", "longitude",
]


def predict_snapshot(feature_dict):
    rf_model, iso_model = get_models()

    if rf_model is None or iso_model is None:
        return {
            "risk_class": "UNKNOWN",
            "risk_probability": 0.0,
            "anomaly_score": 0.0,
        }

    rf_input = pd.DataFrame([{k: feature_dict[k] for k in RF_FEATURES}])
    iso_input = pd.DataFrame([{k: feature_dict[k] for k in ISO_FEATURES}])

    risk_class = str(rf_model.predict(rf_input)[0])
    risk_probability = float(rf_model.predict_proba(rf_input).max())
    anomaly_score = float(-iso_model.score_samples(iso_input)[0])

    return {
        "risk_class": risk_class,
        "risk_probability": risk_probability,
        "anomaly_score": anomaly_score,
    }
