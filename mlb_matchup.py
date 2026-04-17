"""
mlb_matchup.py

Reads mlb_cache.json and computes log5 matchup xBA for every qualified
batter in today's games. Produces a ranked list of the top 20 hitters
and writes it to output.json.

Qualification filter:
    Batter must have appeared in at least 75% of their team's games.

Log5 formula (Bill James):
    expected_ba = (B * P / L) / (B * P / L + (1 - B) * (1 - P) / (1 - L))

    Where:
        B = batter xBA
        P = pitcher / bullpen xBAA
        L = league average xBA (default 0.248)

Matching logic (critical — always opposing teams):
    Home batters  →  Away SP  +  Away bullpen
    Away batters  →  Home SP  +  Home bullpen
"""

import json
import sys
from datetime import date

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CACHE_FILE = "mlb_cache.json"
LEAGUE_AVG_XBA = 0.248        # Tweak as the season progresses
QUALIFY_THRESHOLD = 0.75      # Batter must have played 75% of team games


# ---------------------------------------------------------------------------
# Log5
# ---------------------------------------------------------------------------

def log5(batter_xba: float, pitcher_xbaa: float, league_avg: float = LEAGUE_AVG_XBA) -> float:
    """
    Bill James log5 formula. Returns the expected batting average for
    a specific batter vs a specific pitcher given a league baseline.
    """
    b = batter_xba
    p = pitcher_xbaa
    l = league_avg

    numerator = (b * p) / l
    denominator = numerator + ((1 - b) * (1 - p)) / (1 - l)

    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_cache(path: str = CACHE_FILE) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Cache file '{path}' not found. Run mlb_data_puller.py first.")
        sys.exit(1)


def is_qualified(batter: dict, team_id: str, team_games_played: dict) -> bool:
    """
    Returns True if the batter has played in at least 75% of their
    team's games this season.
    """
    team_gp = team_games_played.get(team_id, 0)
    if team_gp == 0:
        return False
    batter_gp = batter.get("games_played", 0)
    return batter_gp >= (team_gp * QUALIFY_THRESHOLD)


def get_sp_xbaa(sp_id, sp_xbaa: dict, team_id: str, team_overall_xbaa: dict):
    """
    Returns the SP's xBAA. Falls back to the team's overall pitching
    xBAA if no SP is announced or no xBAA data exists for the SP.
    """
    if sp_id is not None:
        val = sp_xbaa.get(str(sp_id))
        if val is not None:
            return float(val), "sp"

    # Fallback: team overall pitching xBAA
    val = team_overall_xbaa.get(str(team_id))
    if val is not None:
        return float(val), "team_overall"

    return None, "none"


# ---------------------------------------------------------------------------
# Main matchup computation
# ---------------------------------------------------------------------------

def compute_matchups(cache: dict) -> list[dict]:
    """
    For every qualified batter in today's games, compute:
      - log5 xBA vs opposing SP (or team overall as fallback)
      - log5 xBA vs opposing bullpen
    Returns a flat list of matchup dicts, unsorted.
    """
    games              = cache["games"]
    batter_xba_map     = cache["batter_xba"]          # str(player_id) -> {...}
    sp_xbaa_map        = cache["sp_xbaa"]              # str(player_id) -> float
    team_overall_xbaa  = cache["team_overall_xbaa"]   # str(team_id)   -> float
    team_bullpen_xbaa  = cache["team_bullpen_xbaa"]   # str(team_id)   -> float
    team_games_played  = cache["team_games_played"]   # str(team_id)   -> int

    results = []

    for game in games:
        home_team_id = game["home_team_id"]
        away_team_id = game["away_team_id"]
        home_sp_id   = game.get("home_sp_id")
        away_sp_id   = game.get("away_sp_id")

        # ------------------------------------------------------------------
        # Define the two sides of the matchup.
        #
        # Each side is: (batting_team_id, opposing_sp_id, opposing_team_id)
        #
        # Home batters face the AWAY pitcher and AWAY bullpen.
        # Away batters face the HOME pitcher and HOME bullpen.
        # ------------------------------------------------------------------
        sides = [
            {
                "batting_team_id":   home_team_id,
                "batting_team_name": game["home_team_name"],
                "opposing_sp_id":    away_sp_id,           # ← away SP faces home batters
                "opposing_sp_name":  game.get("away_sp_name"),
                "opposing_team_id":  away_team_id,          # ← away bullpen faces home batters
                "opposing_team_name":game["away_team_name"],
            },
            {
                "batting_team_id":   away_team_id,
                "batting_team_name": game["away_team_name"],
                "opposing_sp_id":    home_sp_id,            # ← home SP faces away batters
                "opposing_sp_name":  game.get("home_sp_name"),
                "opposing_team_id":  home_team_id,          # ← home bullpen faces away batters
                "opposing_team_name":game["home_team_name"],
            },
        ]

        for side in sides:
            batting_tid   = str(side["batting_team_id"])
            opposing_tid  = str(side["opposing_team_id"])

            # Opposing SP xBAA (with fallback)
            sp_xbaa_val, sp_source = get_sp_xbaa(
                side["opposing_sp_id"],
                sp_xbaa_map,
                side["opposing_team_id"],
                team_overall_xbaa,
            )

            # Opposing bullpen xBAA
            bp_xbaa_val = team_bullpen_xbaa.get(opposing_tid)
            if bp_xbaa_val is not None:
                bp_xbaa_val = float(bp_xbaa_val)

            # Iterate over every batter on the batting team
            for pid, batter in batter_xba_map.items():
                if str(batter["team_id"]) != batting_tid:
                    continue

                # 75% playing time filter
                if not is_qualified(batter, batting_tid, team_games_played):
                    continue

                bxba = batter.get("xba")
                if bxba is None:
                    continue

                # Log5: batter vs SP
                vs_sp_log5 = (
                    log5(bxba, sp_xbaa_val)
                    if sp_xbaa_val is not None
                    else None
                )

                # Log5: batter vs bullpen
                vs_bp_log5 = (
                    log5(bxba, bp_xbaa_val)
                    if bp_xbaa_val is not None
                    else None
                )

                results.append({
                    "player_id":          pid,
                    "name":               batter["name"],
                    "batting_team":       side["batting_team_name"],
                    "opposing_team":      side["opposing_team_name"],
                    "opposing_sp_name":   side["opposing_sp_name"],
                    "sp_xbaa_source":     sp_source,

                    # Raw inputs
                    "batter_xba":         bxba,
                    "sp_xbaa":            sp_xbaa_val,
                    "bullpen_xbaa":       bp_xbaa_val,

                    # Log5 outputs
                    "vs_sp_log5":         vs_sp_log5,
                    "vs_bullpen_log5":    vs_bp_log5,

                    # Playing-time context
                    "games_played":       batter.get("games_played"),
                    "at_bats":            batter.get("at_bats"),
                    "ab_per_game":        batter.get("ab_per_game"),
                })

    return results


# ---------------------------------------------------------------------------
# Hit probability
# ---------------------------------------------------------------------------

def compute_hit_probability(matchup: dict) -> float | None:
    """
    Probability of a batter getting at least one hit in their expected
    at-bats for the day.

    First 2 ABs (or fewer if ab_per_game < 2) use the vs-SP log5 figure.
    Remaining ABs use the vs-bullpen log5 figure.

    P(at least 1 hit) = 1 - P(0 hits in SP ABs) * P(0 hits in BP ABs)
                      = 1 - (1 - vs_sp)^sp_abs * (1 - vs_bp)^bp_abs
    """
    ab_per_game  = matchup.get("ab_per_game") or 0.0
    vs_sp        = matchup.get("vs_sp_log5")
    vs_bp        = matchup.get("vs_bullpen_log5")

    if ab_per_game <= 0:
        return None

    sp_abs = min(2.0, ab_per_game)
    bp_abs = max(0.0, ab_per_game - 2.0)

    # Need at least the SP figure to proceed
    if vs_sp is None:
        return None

    p_no_hit_sp = (1 - vs_sp) ** sp_abs

    # If there are bullpen ABs but no bullpen figure, skip those ABs
    if bp_abs > 0 and vs_bp is not None:
        p_no_hit_bp = (1 - vs_bp) ** bp_abs
    else:
        p_no_hit_bp = 1.0

    return round(1 - (p_no_hit_sp * p_no_hit_bp), 4)


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

CSV_FILE = "batter_probabilities.csv"

CSV_COLUMNS = [
    "name",
    "batting_team",
    "opposing_team",
    "opposing_sp_name",
    "sp_xbaa_source",
    "batter_xba",
    "sp_xbaa",
    "bullpen_xbaa",
    "vs_sp_log5",
    "vs_bullpen_log5",
    "games_played",
    "at_bats",
    "ab_per_game",
    "sp_abs",
    "bp_abs",
    "hit_probability",
]


def write_csv(matchups: list[dict], path: str = CSV_FILE) -> None:
    import csv
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(matchups)
    print(f"  CSV written to {path}  ({len(matchups)} batters)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    print(f"\nMLB Matchup Engine — {date.today().isoformat()}")
    print(f"League avg xBA : {LEAGUE_AVG_XBA}")
    print(f"Qualify cutoff : {int(QUALIFY_THRESHOLD * 100)}% of team games\n")

    cache = load_cache()

    # Confirm cache is current
    cache_date = cache.get("date")
    today = date.today().isoformat()
    if cache_date != today:
        print(f"WARNING: Cache is from {cache_date}, not today ({today}).")

    matchups = compute_matchups(cache)

    # Compute hit probability and AB breakdown for each batter
    for m in matchups:
        ab_per_game = m.get("ab_per_game") or 0.0
        m["sp_abs"]          = round(min(2.0, ab_per_game), 3)
        m["bp_abs"]          = round(max(0.0, ab_per_game - 2.0), 3)
        m["hit_probability"] = compute_hit_probability(m)

    write_csv(matchups)


if __name__ == "__main__":
    run()
