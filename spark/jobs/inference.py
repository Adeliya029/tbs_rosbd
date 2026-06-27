import pandas as pd

from jobs.model_loader import load_models


def predict_snapshot(feature_dict):

    rf_model, iso_model = load_models()

    rf_features = pd.DataFrame([{
        "depth_km":
            feature_dict["depth_km"],

        "quake_count_24h":
            feature_dict["quake_count_24h"],

        "avg_magnitude_24h":
            feature_dict["avg_magnitude_24h"],

        "max_magnitude_24h":
            feature_dict["max_magnitude_24h"],

        "shallow_quake_ratio_24h":
            feature_dict["shallow_quake_ratio_24h"],

        "felt_count_24h":
            feature_dict["felt_count_24h"],

        "swarm_density":
            feature_dict["swarm_density"],

        "latitude":
            feature_dict["latitude"],

        "longitude":
            feature_dict["longitude"]
    }])

    iso_features = pd.DataFrame([{
        "depth_km":
            feature_dict["depth_km"],

        "quake_count_24h":
            feature_dict["quake_count_24h"],

        "avg_magnitude_24h":
            feature_dict["avg_magnitude_24h"],

        "max_magnitude_24h":
            feature_dict["max_magnitude_24h"],

        "shallow_quake_ratio_24h":
            feature_dict["shallow_quake_ratio_24h"],

        "latitude":
            feature_dict["latitude"],

        "longitude":
            feature_dict["longitude"]
    }])

    risk_class = rf_model.predict(
        rf_features
    )[0]

    risk_prob = float(
        rf_model.predict_proba(
            rf_features
        ).max()
    )

    anomaly_score = float(
        -iso_model.score_samples(
            iso_features
        )[0]
    )

    return {
        "risk_class": str(risk_class),
        "risk_probability": risk_prob,
        "anomaly_score": anomaly_score
    }