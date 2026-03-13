"""Privacy zone configuration.

Sensitive home coordinates are loaded from environment variables (set in .env).
Zone radii and optional public anchor points are edited directly in this file.

Anchor points are (latitude, longitude) tuples for a believable public location
where each activity type will appear to start and end — e.g. a park entrance,
road junction, or car park. Set to None to just trim without snapping.

To find anchor coordinates: right-click a point in Google Maps → "What's here?"
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Home location (set these in your .env file) ───────────────────────────────
_lat = os.getenv("HOME_LAT")
_lon = os.getenv("HOME_LON")

if not _lat or not _lon:
    raise RuntimeError(
        "HOME_LAT and HOME_LON must be set in your .env file.\n"
        "Example:\n  HOME_LAT=54.7330\n  HOME_LON=-1.5510"
    )

HOME_LAT: float = float(_lat)
HOME_LON: float = float(_lon)

# ── Per-activity-type privacy zones ───────────────────────────────────────────
# radius_m : strip GPS points within this distance of home at the start and end
# anchor   : (lat, lon) public waypoint the activity will appear to start/end at
#            Set to None to trim only, without snapping to a specific point.
#
# Example anchor (replace with real coordinates from a map):
#   "Ride": {"radius_m": 500, "anchor": (54.7612, -1.5734)},

ZONES: dict[str, dict] = {
    "Ride":          {"radius_m": 500, "anchor": (54.76307, -1.55411)},
    "MountainBikeRide": {"radius_m": 500, "anchor": (54.76307, -1.55411)},
    "Run":           {"radius_m": 300, "anchor": (54.76306, -1.55399)},
    "Walk":          {"radius_m": 200, "anchor": (54.75962, -1.55262)},
    "Hike":          {"radius_m": 200, "anchor": None},
    "default":       {"radius_m": 300, "anchor": None},
}

assert "default" in ZONES, "ZONES must contain a 'default' key"


def get_zone(activity_type: str) -> dict:
    """Return the privacy zone config for a given activity type."""
    return ZONES.get(activity_type) or ZONES["default"]


ZONES = {
      "Ride": {"radius_m": 500, "anchor": (54.76307, -1.55411)},  # e.g. a road junction
      "Run":  {"radius_m": 300, "anchor": (54.76306, -1.55399)},  # e.g. a park entrance
      "Walk": {"radius_m": 200, "anchor": (54.75962, -1.55262)},                 # trim only
  }