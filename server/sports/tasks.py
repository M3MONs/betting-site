import logging
from celery import shared_task
from django.db import transaction
from django.utils import timezone
from datetime import timedelta
from sports.services.parser import ParserFactory
from sports.services.odds_manager import OddsManagerFactory
from sports.models import Match, League
from sports.services.scrapper import ScrapperFactory

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def import_all_league_matches(self, league_id=None, scheduled=False):
    """
    Import matches for all active leagues or a specific league.
    """
    try:
        if league_id:
            leagues = League.objects.filter(id=league_id, is_active=True)
        else:
            leagues = League.objects.filter(is_active=True)

        if not leagues.exists():
            logger.warning("No active leagues found for import.")
            return {"status": "failed", "error": "No active leagues found."}

        results = {
            "total_leagues": leagues.count(),
            "new_matches": 0,
        }

        for league in leagues:
            league_result = import_league_matches(league)
            results["new_matches"] += league_result.get("new_matches", 0)

        logger.info(f"Imported matches for {results['total_leagues']} leagues.")
        return {"status": "success", "results": results}

    except Exception as e:
        error_message = f"Error importing matches: {e}"
        logger.error(error_message, exc_info=True)
        return {"status": "failed", "error": error_message}


def import_league_matches(league):
    """
    Import matches for a specific league.
    """
    logger.info(f"Importing matches for league: {league.name}")

    if not league.source_url:
        logger.warning(f"No source URL for league: {league.name}")
        return {"status": "failed", "error": "No source URL for league."}

    scrapper = ScrapperFactory.create_scrapper(league.data_source)
    parser = ParserFactory.create_parser(league.data_source, sport=league.sport.name)

    if not scrapper:
        logger.warning(f"No scrapper found for data source: {league.data_source}")
        return {"status": "failed", "error": "No scrapper found."}

    try:
        matches = scrapper.get_league_matches(league.source_url)
        matches = parser.parse_fixtures_page(matches)

        if not matches:
            logger.warning(f"No matches found for league: {league.name}")
            return {"status": "failed", "error": "No matches found."}

        new_matches = 0

        with transaction.atomic():
            for match in matches:
                match_exists = Match.objects.filter(
                    source_id=match.get("id"),
                ).exists()

                if not match_exists:
                    match = Match(
                        league=league,
                        home_team=match.get("home_team"),
                        away_team=match.get("away_team"),
                        start_time=match.get("start_time"),
                        source_id=match.get("id"),
                        source_url=match.get("url"),
                    )
                    match.save()
                    new_matches += 1
        logger.info(f"Imported {new_matches} new matches for league: {league.name}")
        return {"status": "success", "new_matches": new_matches}

    except Exception as e:
        logger.error(f"Error importing matches for league {league.name}: {e}", exc_info=True)
        return {"status": "failed", "error": str(e)}


def import_match_odds(match: Match) -> bool:
    """
    Import odds for a specific match.
    """
    if not match.source_url:
        logger.warning(f"No source URL for league: {match.name}")
        return {"status": "failed", "error": "No source URL for league."}

    manager = OddsManagerFactory.create_odds_manager(match.league.data_source, match.league.sport.name)

    if not manager:
        logger.warning(f"No odds manager found for data source: {match.league.data_source} and sport: {match.league.sport.name} ")
        return False

    return manager.import_odds_for_match(match)


@shared_task(bind=True)
def import_upcoming_matches_odds(self):
    """
    Import odds for all matches scheduled to start within the next 3 days.
    This task is scheduled to run every hour.
    """
    logger.info("Starting import of odds for upcoming matches")
    try:
        now = timezone.now()
        three_days_later = now + timedelta(days=3)

        # Get matches that are scheduled to start within 3 days, have source_url and don't have odds yet
        upcoming_matches = Match.objects.filter(start_time__lte=three_days_later, source_url__isnull=False, status__in=["prepared", "scheduled"], is_active=True)

        if not upcoming_matches.exists():
            logger.info("No upcoming matches found for odds import")
            return {"status": "success", "matches_processed": 0}

        results = {"total_matches": upcoming_matches.count(), "successful": 0, "failed": 0}

        for match in upcoming_matches:
            result = import_match_odds(match)
            if result:
                results["successful"] += 1
            else:
                results["failed"] += 1

        logger.info(f"Imported odds for {results['successful']} out of {results['total_matches']} upcoming matches")
        return {"status": "success", "results": results}

    except Exception as e:
        error_message = f"Error importing odds for upcoming matches: {e}"
        logger.error(error_message, exc_info=True)
        return {"status": "failed", "error": error_message}
