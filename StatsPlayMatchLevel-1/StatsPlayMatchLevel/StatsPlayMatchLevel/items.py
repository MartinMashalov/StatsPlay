# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime
from typing import Iterable


def time_update_serializer(update_desc: str) -> str:
    """change the date updated to a datetime measure in python"""
    # change the update time description
    update_desc = update_desc.split(': ')[-1]

    # Use datetime.strptime to parse the string into a datetime object
    date_object = datetime.strptime(update_desc, "%B %d, %Y")

    # Convert the datetime object to a string in your desired format
    formatted_date = date_object.strftime("%Y-%m-%d")
    return formatted_date


def team_serializer(teams: Iterable) -> Iterable:
    """parse through the teams and remove any headers that do not belong to a group of teams"""
    return [team for team in teams if 'division' not in team.lower()]


# collection of MLB items
class MLBStandingsItem(scrapy.Item):
    """define the general fields for the data scraped from season standings"""
    key = scrapy.Field()
    sport = scrapy.Field()
    teams = scrapy.Field(serializer=team_serializer)
    american_league = scrapy.Field()
    national_league = scrapy.Field()


class MLBLeadersItem(scrapy.Item):
    """define the general fields for the leaders in each area of the MLB league ex. home runs"""
    key = scrapy.Field()
    hits = scrapy.Field()
    sport = scrapy.Field()
    home_runs = scrapy.Field()
    doubles = scrapy.Field()
    triples = scrapy.Field()
    rbi = scrapy.Field()
    wins = scrapy.Field()
    shutouts = scrapy.Field()
    strikeouts = scrapy.Field()
    saves = scrapy.Field()


class MLBTeamsItem(scrapy.Item):
    """define the fields for the data collections on the teams in mlb previously scraped"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    pitching = scrapy.Field()
    sport = scrapy.Field()
    batting = scrapy.Field()
    fielding = scrapy.Field()


class MLBRosterItem(scrapy.Item):
    """define the general parameters and fields for the roster scraping process"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    sport = scrapy.Field()
    roster = scrapy.Field()


class MLBPlayerItem(scrapy.Item):
    """general fields defined for the player scraping process"""
    key = scrapy.Field()
    sport = scrapy.Field()
    description = scrapy.Field()
    pitching = scrapy.Field()
    batting = scrapy.Field()
    fielding = scrapy.Field()


# collection of NBA items

class NBAStandingsItem(scrapy.Item):
    """define the general fields for the data scraped from season standings"""
    key = scrapy.Field()
    sport = scrapy.Field()
    teams = scrapy.Field(serializer=team_serializer)
    NBA = scrapy.Field()


class NBALeadersItem(scrapy.Item):
    """define the general fields for the leaders in each area of the MLB league ex. home runs"""
    key = scrapy.Field()
    points = scrapy.Field()
    sport = scrapy.Field()
    rebounds = scrapy.Field()
    assists = scrapy.Field()
    steals = scrapy.Field()
    blocks = scrapy.Field()


class NBATeamsItem(scrapy.Item):
    """define the fields for the data collections on the teams in mlb previously scraped"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    sport = scrapy.Field()
    totals = scrapy.Field()
    averages = scrapy.Field()
    postseason_totals = scrapy.Field()
    postseason_averages = scrapy.Field()


class NBARosterItem(scrapy.Item):
    """define the general parameters and fields for the roster scraping process"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    sport = scrapy.Field()
    roster = scrapy.Field()


class NBAPlayerItem(scrapy.Item):
    """general fields defined for the player scraping process"""
    key = scrapy.Field()
    description = scrapy.Field()
    sport = scrapy.Field()
    totals = scrapy.Field()
    averages = scrapy.Field()
    post_totals = scrapy.Field()
    post_averages = scrapy.Field()


# collection of NFL items

class NFLStandingsItem(scrapy.Item):
    """define the general fields for the data scraped from season standings"""
    key = scrapy.Field()
    sport = scrapy.Field()
    teams = scrapy.Field(serializer=team_serializer)
    NFL = scrapy.Field()


class NFLLeadersItem(scrapy.Item):
    """define the general fields for the leaders in each area of the MLB league ex. home runs"""
    key = scrapy.Field()
    sport = scrapy.Field()
    passing_touchdowns = scrapy.Field()
    passing_yardage = scrapy.Field()
    rushing_yardage = scrapy.Field()
    rushing_touchdowns = scrapy.Field()
    receiving = scrapy.Field()
    receiving_yards = scrapy.Field()
    receiving_touchdowns = scrapy.Field()
    interceptions = scrapy.Field()
    sacks = scrapy.Field()


class NFLTeamsItem(scrapy.Item):
    """define the fields for the data collections on the teams in mlb previously scraped"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    sport = scrapy.Field()
    passing = scrapy.Field()
    rushing = scrapy.Field()
    receiving = scrapy.Field()
    kicking = scrapy.Field()
    punting = scrapy.Field()
    punts_returned = scrapy.Field()
    kicks_returned = scrapy.Field()
    interceptions = scrapy.Field()
    sacks = scrapy.Field()
    defense = scrapy.Field()
    points = scrapy.Field()


class NFLRosterItem(scrapy.Item):
    """define the general parameters and fields for the roster scraping process"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    sport = scrapy.Field()
    roster = scrapy.Field()


class NFLPlayerItem(scrapy.Item):
    """general fields defined for the player scraping process"""
    key = scrapy.Field()
    description = scrapy.Field()
    sport = scrapy.Field()
    passing = scrapy.Field()
    rushing = scrapy.Field()
    receiving = scrapy.Field()
    kicking = scrapy.Field()
    punting = scrapy.Field()
    punts_returned = scrapy.Field()
    kicks_returned = scrapy.Field()
    interceptions = scrapy.Field()
    sacks = scrapy.Field()
    defense = scrapy.Field()
    scoring = scrapy.Field()
    career = scrapy.Field()


# collections of NHL items

class NHLStandingsItem(scrapy.Item):
    """define the general fields for the data scraped from season standings"""
    key = scrapy.Field()
    sport = scrapy.Field()
    teams = scrapy.Field(serializer=team_serializer)
    NHL = scrapy.Field()


class NHLLeadersItem(scrapy.Item):
    """define the general fields for the leaders in each area of the MLB league ex. home runs"""
    key = scrapy.Field()
    sport = scrapy.Field()
    points = scrapy.Field()
    goals = scrapy.Field()
    assists = scrapy.Field()
    penalties = scrapy.Field()
    wins = scrapy.Field()
    shutouts = scrapy.Field()


class NHLTeamsItem(scrapy.Item):
    """define the fields for the data collections on the teams in mlb previously scraped"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    sport = scrapy.Field()
    scoring = scrapy.Field()
    goaltending = scrapy.Field()


class NHLRosterItem(scrapy.Item):
    """define the general parameters and fields for the roster scraping process"""
    updated = scrapy.Field(serializer=time_update_serializer)
    key = scrapy.Field()
    sport = scrapy.Field()
    roster = scrapy.Field()


class NHLPlayerItem(scrapy.Item):
    """general fields defined for the player scraping process"""
    key = scrapy.Field()
    description = scrapy.Field()
    sport = scrapy.Field()
    scoring = scrapy.Field()
    postseason_scoring = scrapy.Field()
    goaltending = scrapy.Field()
    postseason_goaltending = scrapy.Field()