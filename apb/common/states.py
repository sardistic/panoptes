"""US state/territory centroids — coarse fallback geolocation.

County/state-granularity sources (OpenFEMA declarations, FAA TFRs without a
resolvable city) have no point geometry. When a finer place match fails we drop
the event onto its state centroid so it still renders and clusters by region.
"""
from __future__ import annotations

# (lat, lon) approximate geographic center of each state/territory.
STATE_CENTROIDS: dict[str, tuple[float, float]] = {
    "AL": (32.806, -86.791), "AK": (61.370, -152.404), "AZ": (33.729, -111.431),
    "AR": (34.970, -92.373), "CA": (36.117, -119.682), "CO": (39.059, -105.311),
    "CT": (41.598, -72.755), "DE": (39.319, -75.507), "DC": (38.897, -77.026),
    "FL": (27.766, -81.687), "GA": (33.040, -83.643), "HI": (21.094, -157.498),
    "ID": (44.240, -114.478), "IL": (40.349, -88.986), "IN": (39.849, -86.258),
    "IA": (42.011, -93.210), "KS": (38.526, -96.726), "KY": (37.668, -84.670),
    "LA": (31.169, -91.867), "ME": (44.693, -69.382), "MD": (39.064, -76.741),
    "MA": (42.230, -71.530), "MI": (43.327, -84.536), "MN": (45.694, -93.900),
    "MS": (32.741, -89.678), "MO": (38.456, -92.288), "MT": (46.921, -110.454),
    "NE": (41.125, -98.268), "NV": (38.313, -117.055), "NH": (43.452, -71.564),
    "NJ": (40.299, -74.521), "NM": (34.841, -106.248), "NY": (42.166, -74.948),
    "NC": (35.630, -79.806), "ND": (47.528, -99.784), "OH": (40.388, -82.764),
    "OK": (35.565, -96.929), "OR": (44.572, -122.071), "PA": (40.590, -77.209),
    "RI": (41.680, -71.512), "SC": (33.856, -80.945), "SD": (44.299, -99.438),
    "TN": (35.747, -86.692), "TX": (31.054, -97.563), "UT": (40.150, -111.862),
    "VT": (44.045, -72.710), "VA": (37.770, -78.170), "WA": (47.401, -121.490),
    "WV": (38.491, -80.954), "WI": (44.268, -89.616), "WY": (42.756, -107.302),
    "PR": (18.220, -66.590), "VI": (18.336, -64.896), "GU": (13.444, 144.794),
}


def state_centroid(state: str | None) -> tuple[float | None, float | None]:
    if not state:
        return None, None
    return STATE_CENTROIDS.get(state.strip().upper(), (None, None))
