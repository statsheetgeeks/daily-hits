"""
mlb_data_puller.py

Pulls all MLB data needed for the hitter matchup model and stores it in
a local JSON cache. Intended to be run once daily by GitHub Actions.
Previous day's cache is overwritten on each run.

Data collected:
  - Today's games with probable starting pitchers
  - Active batters' xBA, games played, at bats, and AB/game
  - Starting pitchers' xBAA
  - Each team's overall pitching xBAA (fallback when no SP listed)
  - Each team's bullpen xBAA
  - Each team's games played (for 75% qualification filter)

Requirements:
    pip install python-mlb-statsapi
"""

import json
import os
import time
import mlbstatsapi
from datetime import date, datetime

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CACHE_FILE = "mlb_cache.json"
SEASON = 2026

# Pitchers with fewer than this many starts are treated as relievers.
# Adjust as the season progresses.
STARTER_THRESHOLD = 3

# Brief pause between API calls to avoid hammering the endpoint.
API_DELAY = 0.15  # seconds

mlb = mlbstatsapi.Mlb()


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def save_cache(data: dict) -> None:
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Cache saved to {CACHE_FILE}")


def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_todays_games() -> list[dict]:
    """
    Returns today's games with probable starter IDs for each side.
    Probable pitcher may be None if not yet announced.
    """
    today_str = date.today().strftime("%Y-%m-%d")
    print(f"  Fetching schedule for {today_str}...")

    try:
        sched = mlb.get_schedule(date=today_str, sportId=1)
    except Exception as e:
        print(f"  ERROR fetching schedule: {e}")
        return []

    games = []
    for date_obj in sched.dates:
        for game in date_obj.games:
            # Only include regular season games
            if game.game_type != "R":
                continue

            info = {
                "game_pk": game.game_pk,
                "home_team_id": game.teams.home.team.id,
                "home_team_name": game.teams.home.team.name,
                "away_team_id": game.teams.away.team.id,
                "away_team_name": game.teams.away.team.name,
                "home_sp_id": None,
                "home_sp_name": None,
                "away_sp_id": None,
                "away_sp_name": None,
            }

            home_pp = getattr(game.teams.home, "probable_pitcher", None)
            if home_pp:
                info["home_sp_id"] = home_pp.id
                info["home_sp_name"] = getattr(home_pp, "full_name", str(home_pp.id))

            away_pp = getattr(game.teams.away, "probable_pitcher", None)
            if away_pp:
                info["away_sp_id"] = away_pp.id
                info["away_sp_name"] = getattr(away_pp, "full_name", str(away_pp.id))

            games.append(info)

    print(f"  Found {len(games)} regular season game(s).")
    return games


def fetch_batter_xba(team_ids: set) -> dict:
    """
    For every non-pitcher on each team's active roster, fetch xBA.
    Returns: { player_id (str) -> { name, team_id, xba } }
    """
    print(f"  Fetching batter xBA for {len(team_ids)} team(s)...")
    batter_xba = {}

    for team_id in team_ids:
        try:
            roster = mlb.get_team_roster(team_id, rosterType="active")
        except Exception as e:
            print(f"    ERROR fetching roster for team {team_id}: {e}")
            continue

        # API returns either an object with .roster or a plain list
        players = roster.roster if hasattr(roster, "roster") else roster

        for player in players:
            if player.position.abbreviation == "P":
                continue

            pid = player.person.id
            name = getattr(player.person, "full_name", str(pid))

            try:
                stats = mlb.get_player_stats(
                    pid,
                    stats=["expectedStatistics"],
                    groups=["hitting"],
                    season=SEASON,
                )
                if "hitting" not in stats or "expectedStatistics" not in stats["hitting"]:
                    continue
                for split in stats["hitting"]["expectedStatistics"].splits:
                    raw = getattr(split.stat, "avg", None)
                    if raw is not None:
                        batter_xba[str(pid)] = {
                            "name": name,
                            "team_id": team_id,
                            "xba": float(raw),
                        }
            except Exception as e:
                print(f"    ERROR fetching xBA for {name}: {e}")

            time.sleep(API_DELAY)

    print(f"  Collected xBA for {len(batter_xba)} batter(s).")
    return batter_xba


def fetch_sp_xbaa(sp_ids: set) -> dict:
    """
    Fetch xBAA for each probable starting pitcher.
    Returns: { player_id (str) -> xbaa (float) }
    """
    print(f"  Fetching xBAA for {len(sp_ids)} starting pitcher(s)...")
    sp_xbaa = {}

    for pid in sp_ids:
        if pid is None:
            continue
        try:
            stats = mlb.get_player_stats(
                pid,
                stats=["expectedStatistics"],
                groups=["pitching"],
                season=SEASON,
            )
            if "pitching" not in stats or "expectedStatistics" not in stats["pitching"]:
                continue
            for split in stats["pitching"]["expectedStatistics"].splits:
                raw = getattr(split.stat, "avg", None)
                if raw is not None:
                    sp_xbaa[str(pid)] = float(raw)
        except Exception as e:
            print(f"    ERROR fetching xBAA for SP {pid}: {e}")

        time.sleep(API_DELAY)

    print(f"  Collected xBAA for {len(sp_xbaa)} starter(s).")
    return sp_xbaa


def fetch_team_pitching_xbaa(team_ids: set) -> tuple[dict, dict]:
    """
    For each team, aggregates xBAA across all pitchers (weighted by batters
    faced) to produce:
      - overall team pitching xBAA  (used as fallback when no SP announced)
      - bullpen-only xBAA           (relievers: games_started < STARTER_THRESHOLD)

    Returns: (team_overall_xbaa, team_bullpen_xbaa)
      Both are dicts: { team_id (str) -> xbaa (float | None) }
    """
    print(f"  Fetching team pitching xBAA for {len(team_ids)} team(s)...")
    team_overall: dict = {}
    team_bullpen: dict = {}

    for team_id in team_ids:
        try:
            roster = mlb.get_team_roster(team_id, rosterType="active")
        except Exception as e:
            print(f"    ERROR fetching roster for team {team_id}: {e}")
            team_overall[str(team_id)] = None
            team_bullpen[str(team_id)] = None
            continue

        overall_num = 0.0
        overall_den = 0
        bullpen_num = 0.0
        bullpen_den = 0

        # API returns either an object with .roster or a plain list
        players = roster.roster if hasattr(roster, "roster") else roster

        for player in players:
            if player.position.abbreviation != "P":
                continue

            pid = player.person.id
            name = getattr(player.person, "full_name", str(pid))

            try:
                # Step 1: season stats to get games_started and batters_faced
                season_stats = mlb.get_player_stats(
                    pid,
                    stats=["season"],
                    groups=["pitching"],
                    season=SEASON,
                )
                games_started = 0
                batters_faced = 0
                if "pitching" in season_stats and "season" in season_stats["pitching"]:
                    for split in season_stats["pitching"]["season"].splits:
                        games_started = getattr(split.stat, "games_started", 0) or 0
                        batters_faced = getattr(split.stat, "batters_faced", 0) or 0

                if batters_faced == 0:
                    time.sleep(API_DELAY)
                    continue

                # Step 2: xBAA
                xbaa_stats = mlb.get_player_stats(
                    pid,
                    stats=["expectedStatistics"],
                    groups=["pitching"],
                    season=SEASON,
                )
                xba = None
                if "pitching" in xbaa_stats and "expectedStatistics" in xbaa_stats["pitching"]:
                    for split in xbaa_stats["pitching"]["expectedStatistics"].splits:
                        raw = getattr(split.stat, "avg", None)
                        if raw is not None:
                            xba = float(raw)

                if xba is None:
                    time.sleep(API_DELAY)
                    continue

                # Accumulate overall
                overall_num += xba * batters_faced
                overall_den += batters_faced

                # Accumulate bullpen
                if games_started < STARTER_THRESHOLD:
                    bullpen_num += xba * batters_faced
                    bullpen_den += batters_faced

            except Exception as e:
                print(f"    ERROR processing pitcher {name}: {e}")

            time.sleep(API_DELAY)

        team_overall[str(team_id)] = overall_num / overall_den if overall_den > 0 else None
        team_bullpen[str(team_id)] = bullpen_num / bullpen_den if bullpen_den > 0 else None

        bp_val = team_bullpen[str(team_id)]
        ov_val = team_overall[str(team_id)]
        print(f"    Team {team_id} — overall xBAA: {ov_val:.3f if ov_val else 'N/A'}, "
              f"bullpen xBAA: {bp_val:.3f if bp_val else 'N/A'}")

    return team_overall, team_bullpen


# ---------------------------------------------------------------------------
# Team games played (for 75% qualification filter)
# ---------------------------------------------------------------------------

def fetch_team_games_played(team_ids: set) -> dict:
    """
    Returns the number of games each team has played this season,
    pulled from standings. Used for the 75% playing-time filter.
    Returns: { team_id (str) -> games_played (int) }
    """
    print("  Fetching team games played from standings...")
    team_gp = {}

    # League IDs: 103 = AL, 104 = NL
    for league_id in [103, 104]:
        try:
            standings = mlb.get_standings(leagueId=league_id, season=SEASON)
            for record in standings.records:
                for team_record in record.team_records:
                    tid = str(team_record.team.id)
                    gp = (team_record.wins or 0) + (team_record.losses or 0)
                    team_gp[tid] = gp
        except Exception as e:
            print(f"    ERROR fetching standings for league {league_id}: {e}")

    print(f"  Got games played for {len(team_gp)} team(s).")
    return team_gp


# ---------------------------------------------------------------------------
# Main daily job
# ---------------------------------------------------------------------------

def run_daily_pull() -> None:
    start = datetime.now()
    print(f"\n{'='*60}")
    print(f"MLB Data Pull — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    today = date.today().isoformat()

    # --- Games ---
    games = fetch_todays_games()
    if not games:
        print("No games today. Exiting pull.")
        return

    # Unique team IDs and SP IDs from today's slate
    team_ids: set[int] = set()
    sp_ids: set[int] = set()
    for g in games:
        team_ids.update([g["home_team_id"], g["away_team_id"]])
        if g["home_sp_id"]:
            sp_ids.add(g["home_sp_id"])
        if g["away_sp_id"]:
            sp_ids.add(g["away_sp_id"])

    # --- Individual player data ---
    batter_xba = fetch_batter_xba(team_ids)
    sp_xbaa = fetch_sp_xbaa(sp_ids)

    # --- Team pitching aggregates ---
    team_overall_xbaa, team_bullpen_xbaa = fetch_team_pitching_xbaa(team_ids)

    # --- Team games played (for 75% qualification filter) ---
    team_games_played = fetch_team_games_played(team_ids)

    # --- Build and save cache ---
    cache = {
        "date": today,
        "season": SEASON,
        "games": games,
        "batter_xba": batter_xba,                # keyed by str(player_id)
        "sp_xbaa": sp_xbaa,                      # keyed by str(player_id)
        "team_overall_xbaa": team_overall_xbaa,  # keyed by str(team_id)
        "team_bullpen_xbaa": team_bullpen_xbaa,  # keyed by str(team_id)
        "team_games_played": team_games_played,  # keyed by str(team_id)
    }
    save_cache(cache)

    elapsed = (datetime.now() - start).seconds
    print(f"\nPull complete in {elapsed}s.")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Entry point — runs once and exits (scheduling handled by GitHub Actions)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_daily_pull()
