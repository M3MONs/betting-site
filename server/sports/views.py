from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.pagination import PageNumberPagination
from rest_framework import status

from sports.repositories.BetSlipRepository import BetSlipRepository, InsufficientBalanceError, MatchNotAvailableError
from sports.repositories.LeagueRepository import LeagueRepository
from sports.repositories.MatchRepository import MatchRepository
from sports.repositories.SportRepository import SportRepository

from .models import Match, League
from .serializers import (
    BetSlipCreateSerializer,
    BetSlipResponseSerializer,
    LeagueDetailSerializer,
    LeagueSerializer,
    MatchSerializer,
    SportWithLeaguesSerializer,
    UserBetSlipSerializer,
)
from .swagger_docs import (
    popular_leagues_schema,
    sports_with_leagues_schema,
    list_matches_schema,
    bet_slip_create_schema,
    user_bet_slips_schema,
    validate_bets_schema,
    league_details_schema,
    league_matches_schema,
)


@popular_leagues_schema
@api_view(["GET"])
def popular_leagues(request) -> Response:
    """
    Get list of popular leagues
    """
    upcoming_matches = MatchRepository.get_upcoming_matches()
    leagues = LeagueRepository.get_popular_leagues(upcoming_matches)

    serializer = LeagueSerializer(leagues, many=True)
    return Response(serializer.data)


@sports_with_leagues_schema
@api_view(["GET"])
def sports_with_leagues(request) -> Response:
    """
    Get list of sports with leagues
    """
    upcoming_matches = MatchRepository.get_upcoming_matches()
    leagues_with_matches = LeagueRepository.get_leagues(upcoming_matches)
    sports = SportRepository.get_sports(leagues_with_matches)

    serializer = SportWithLeaguesSerializer(sports, many=True)
    return Response(serializer.data)


@list_matches_schema
@api_view(["GET"])
def list_popular_matches(request) -> Response:
    """
    Get list of upcoming popular matches
    """
    matches = MatchRepository.get_popular_matches()

    serializer = MatchSerializer(matches, many=True)
    return Response(serializer.data)


@bet_slip_create_schema
@api_view(["POST"])
@permission_classes([IsAuthenticated])
def create_bet_slip(request) -> Response:
    """
    Create a bet slip
    """
    serializer = BetSlipCreateSerializer(data=request.data, context={"request": request})

    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    try:
        bet_slip, user_balance = BetSlipRepository.create_bet_slip(user=request.user, validated_data=serializer.validated_data)
    except InsufficientBalanceError:
        return Response({"error": "Insufficient balance"}, status=status.HTTP_400_BAD_REQUEST)
    except MatchNotAvailableError as e:
        return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

    response_serializer = BetSlipResponseSerializer(bet_slip)
    response_data = response_serializer.data
    response_data["user_balance"] = user_balance

    return Response(response_data, status=status.HTTP_201_CREATED)


class BetPagination(PageNumberPagination):
    page_size = 5
    page_size_query_param = "page_size"
    max_page_size = 10


@user_bet_slips_schema
@api_view(["GET"])
@permission_classes([IsAuthenticated])
def user_bet_slips(request) -> Response:
    """
    Get user's bet slips with optional filtering by status
    """
    status_filter = request.query_params.get("status")
    user_bet_slips = BetSlipRepository.get_user_bet_slips(request.user, status_filter)

    paginator = BetPagination()
    page = paginator.paginate_queryset(user_bet_slips, request)

    if page is not None:
        serializer = UserBetSlipSerializer(page, many=True)
        return paginator.get_paginated_response(serializer.data)

    serializer = UserBetSlipSerializer(user_bet_slips, many=True)
    return Response(serializer.data)


@validate_bets_schema
@api_view(["POST"])
def validate_bets_availability(request) -> Response:
    """
    Validate if bets are available
    """
    match_ids = request.data.get("matchIds", [])

    # If no match IDs are provided, return an empty response as all bets are available
    if not match_ids:
        return Response([], status=status.HTTP_200_OK)

    matches = MatchRepository.get_matches_by_ids(match_ids)

    # Return only matches that are available for betting
    response_data = []
    for match in matches:
        if match.can_bet:
            response_data.append(
                {
                    "id": match.id,
                    "can_bet": match.can_bet,
                    "status": match.status,
                    "start_time": match.start_time,
                    "home_team": match.home_team,
                    "away_team": match.away_team,
                }
            )

    return Response(response_data, status=status.HTTP_200_OK)


@league_details_schema
@api_view(["GET"])
def league_details(request, league_slug) -> Response:
    """
    Get details of a specific league
    """
    try:
        league = LeagueRepository.get_active_league(league_slug)
    except League.DoesNotExist:
        return Response({"error": "League not found"}, status=status.HTTP_404_NOT_FOUND)

    serializer = LeagueDetailSerializer(league)
    return Response(serializer.data)


@league_matches_schema
@api_view(["GET"])
def league_matches(request, league_slug) -> Response:
    """
    Get matches for a specific league
    """
    try:
        league = LeagueRepository.get_active_league(league_slug)
    except League.DoesNotExist:
        return Response({"error": "League not found"}, status=status.HTTP_404_NOT_FOUND)

    matches = MatchRepository.get_league_matches(league)

    serializer = MatchSerializer(matches, many=True)
    return Response(serializer.data)


@api_view(["GET"])
def single_match(request, match_id) -> Response:
    """
    Get details of a single match
    """
    try:
        match = MatchRepository.get_active_match(match_id)
    except Match.DoesNotExist:
        return Response({"error": "Match not found"}, status=status.HTTP_404_NOT_FOUND)

    serializer = MatchSerializer(match)
    return Response(serializer.data)
