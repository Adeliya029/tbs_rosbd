import numpy as np
import pandas as pd


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0

    lat1, lon1, lat2, lon2 = map(
        np.radians,
        [lat1, lon1, lat2, lon2]
    )

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1)
        * np.cos(lat2)
        * np.sin(dlon / 2) ** 2
    )

    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


def detect_swarm(
    df,
    eps1=50,
    eps2=24,
    min_samples=5,
    magnitude_threshold=5.0
):
    """
    ST-DBSCAN: Spatio-Temporal DBSCAN
    untuk deteksi earthquake swarm.

    Parameter (sesuai Bab 8.1 dokumen requirement):
    ----------
    df : pd.DataFrame
        Kolom wajib: event_time, latitude, longitude, magnitude
    eps1 : float
        Threshold spasial dalam km (default 50 km)
    eps2 : float
        Threshold temporal dalam jam (default 24 jam)
    min_samples : int
        Minimum event untuk membentuk cluster (default 5)
    magnitude_threshold : float
        Hanya event dengan magnitude < threshold (default 5.0)

    Return:
    -------
    swarm_density : float
        Jumlah cluster swarm yang terdeteksi
    """

    n_total = len(df)

    if n_total < min_samples:
        return 0.0

    df = df[
        df["magnitude"] < magnitude_threshold
    ].copy()

    n = len(df)

    if n < min_samples:
        return 0.0

    lats = df["latitude"].values
    lons = df["longitude"].values

    times = pd.to_datetime(df["event_time"])
    hours = (
        (times - times.min())
        .dt.total_seconds()
        .values
        / 3600.0
    )

    neighbors = []

    for i in range(n):

        nbrs = []

        for j in range(n):

            if i == j:
                continue

            dist = _haversine_km(
                lats[i], lons[i],
                lats[j], lons[j]
            )

            if dist > eps1:
                continue

            time_diff = abs(
                hours[i] - hours[j]
            )

            if time_diff > eps2:
                continue

            nbrs.append(j)

        neighbors.append(nbrs)

    labels = np.full(n, -1)
    cluster_id = 0

    for i in range(n):

        if labels[i] != -1:
            continue

        if len(neighbors[i]) < min_samples:
            continue

        labels[i] = cluster_id
        seed_set = set(neighbors[i])

        while seed_set:

            j = seed_set.pop()

            if labels[j] == -1:

                labels[j] = cluster_id

                if len(neighbors[j]) >= min_samples:
                    seed_set.update(neighbors[j])

        cluster_id += 1

    cluster_count = len(
        set(labels)
    ) - (
        1 if -1 in labels else 0
    )

    return float(cluster_count)
