from django.db.models import Q, OuterRef
from django.db.models.manager import BaseManager
from django.utils import timezone
from sports.models import Match


class MatchRepository:
    @staticmethod
    def get_upcoming_matches() -> BaseManager[Match]:
        """
        Get upcoming matches that are active and available for betting.
        """
        now = timezone.now()

        return Match.objects.filter(league=OuterRef("pk"), is_active=True, is_bet_available=True).filter(
            Q(status="live") | Q(status="scheduled", start_time__gte=now)
        )

    @staticmethod
    def get_popular_matches() -> BaseManager[Match]:
        """
        Get popular matches that are active and available for betting.
        """
        now = timezone.now()

        return (
            Match.objects.filter(is_active=True, is_popular=True)
            .filter(Q(status="live") | Q(status="scheduled", start_time__gte=now))
            .order_by("-status", "start_time")[:15]
        )

    @staticmethod
    def get_league_matches(league: str) -> BaseManager[Match]:
        """
        Get matches for a specific league that are active and available for betting.
        """
        now = timezone.now()

        return (
            Match.objects.filter(league=league, is_active=True, is_bet_available=True)
            .filter(Q(status="live") | Q(status="scheduled", start_time__gte=now))
            .order_by("-status", "start_time")
        )
    
    @staticmethod 
    def get_active_match(match_id: int) -> Match:
        """
        Get a single active match by its ID.
        """
        return Match.objects.get(id=match_id, is_active=True)


    @staticmethod
    def get_matches_by_ids(match_ids: list[int]) -> BaseManager[Match]:
        """
        Get matches by their IDs that are active and available for betting.
        """

        return Match.objects.filter(id__in=match_ids)