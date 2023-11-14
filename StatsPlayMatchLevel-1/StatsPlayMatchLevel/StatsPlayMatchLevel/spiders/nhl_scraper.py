"""scraper for NFL matches"""

import scrapy
from typing import Any, List
from pydantic import BaseModel
from pprint import pprint
from datetime import datetime
from ..items import NHLStandingsItem, NHLLeadersItem, NHLTeamsItem, NHLPlayerItem, NHLRosterItem
from .models import find_most_similar_key


class KeywordGeneral(BaseModel):
    """general header names for whole site to avoid local errors"""

    # team stats
    team_scoring: list = ["player", "position", "gp", "g", "a", "points", "pim", "pos_neg", "ppg", "ppa", "shg", "sha",
                          "gwg","gtg", "sog", "sg_perc", "toi", "atoi"]

    team_goaltending: list = ["player", "gp", "min", "w", "l", "t_ol", "eng", "sho", "ga", "gaa", "shts", "svs",
                              "sv_perc"]
    # roster
    roster: list = ["player", "position", "birth_date", "height", "weight", "s_c", "origin"]
    # player stats
    player_scoring: list = ["year", "lg", "team", "gp", "g", "a", "points", "pim", "pos_neg", "ppg", "ppa", "shg","sha",
                            "gwg", "gtg", "sog", "sg_perc"]
    player_goaltending: list = ["year", "lg", "team", "gp", "min", "w", "l", "t_ol", "eng", "sho", "ga", "gaa",
                                "shts", "svs", "sv_perc"]

    # standings
    regular_season: list = ["team", "w", "l", "otl", "points", "gf", "ga"]
    # statistical leaders
    points: list = ["player", "team", "points", "g", "a"]
    goals: list = ["player", "team", "g", "a", "points"]
    assists: list = ["player", "team", "a", "g", "points"]
    penalties: list = ["player", "team", "pim"]
    wins: list = ["player", "team", "w", "l", "t"]
    shutouts: list = ["player", "team", "sho", "w", "l", "t"]


general_model = KeywordGeneral()


# scraping key base models
class ScrapingKeysMicro(BaseModel):
    """scraping keys for the scraping process hardcoded"""

    # totals  mappings
    scoring_idx: dict = {i: general_model.team_scoring[i-1] for i in range(1, len(general_model.team_scoring) + 1)}
    goaltending_idx: dict = {i: general_model.team_goaltending[i - 1] for i in range(1, len(
        general_model.team_goaltending) + 1)}

    # map it to each keyword in the output headers
    keyword_idx_teams: dict = {
        'scoring': scoring_idx,
        'goaltending': goaltending_idx
    }

    # roster mappings
    roster_idx: dict = {i: general_model.roster[i - 1].lower() for i in range(1, len(general_model.roster) + 1)}

    # player level statistics
    player_scoring: dict = {i: general_model.player_scoring[i - 1] for i in
                                range(1, len(general_model.player_scoring) + 1)}
    player_goaltending: dict = {i: general_model.player_goaltending[i - 1] for i in
                                range(1, len(general_model.player_goaltending) + 1)}

    # create the overall player keyword indices
    player_keyword_idx: dict = {
        'scoring': player_scoring,
        'postseason_scoring': player_scoring,
        'goaltending': player_goaltending,
        'postseason_goaltending': player_goaltending
    }


class ScrapingKeysMacro(BaseModel):
    """scraping keys and mappings for the standings and leaders macro level stats"""

    # standings mapping
    regular_season_idx: dict = {i: general_model.regular_season[i - 1] for i in range(1,
                                                                                len(general_model.regular_season) + 1)}
    # define the general keyword mappings
    standings_keywords: dict = {
        'NHL': regular_season_idx,
    }

    # leaders mappings
    points_idx: dict = {i: general_model.points[i - 1] for i in
                                    range(1, len(general_model.points) + 1)}
    goals_idx: dict = {i: general_model.goals[i - 1] for i in
                                 range(1, len(general_model.goals) + 1)}
    assists_idx: dict = {i: general_model.assists[i - 1] for i in
                                 range(1, len(general_model.assists) + 1)}
    penalties_idx: dict = {i: general_model.penalties[i - 1] for i in
                                    range(1, len(general_model.penalties) + 1)}
    wins_idx: dict = {i: general_model.wins[i - 1] for i in range(1, len(general_model.wins) + 1)}
    shutouts_idx: dict = {i: general_model.shutouts[i - 1] for i in
                                 range(1, len(general_model.shutouts) + 1)}

    # team leaders stats keywords
    leaders_keyword_idx: dict = {
        'points': points_idx,
        'goals': goals_idx,
        'assists': assists_idx,
        'penalties': penalties_idx,
        'wins': wins_idx,
        'shutouts': shutouts_idx
    }


class MatchNHLScraper(scrapy.Spider):
    """match level scraper for the mlb league in USA"""

    # class variables for naming and starting domain
    sport: str = 'nhl'
    general_sport: str = 'hockey'
    name: str = f'{sport}_matches'
    start_urls: List[str] = ['https://www.statscrew.com/hockey/l-NHL/y-2023']

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        # initiate the settings models with the mappings and keyword settings
        self.settings_micro = ScrapingKeysMicro()
        self.settings_macro = ScrapingKeysMacro()

        # roster links storage
        self.roster_links: list = []

    @staticmethod
    def _check_postseason_presence(html_arr: list, search_key: str) -> bool:
        """check if team or player has had postseason presence on their profile"""

        # build the entity summary string to check for string expressions
        summary_str: str = ' '.join([str(html).lower() for html in html_arr])

        # check if the search key is in the string
        if search_key in summary_str:
            return True
        else:
            return False

    def _updated_date(self, response) -> str:
        """handle the updated date"""

        # get preliminary search
        updated_date: str = response.xpath("//p[@class='agate']/i/text()").get()

        # check if the updated date is missing
        if not updated_date:
            # fetch the current date
            current_date = datetime.now()

            # format the updated date to be in the proper form
            updated_date = current_date.strftime("%B %d, %Y")

        return updated_date

    @staticmethod
    def _create_idx(idx_arrays: list) -> list:
        """create index of player reference keys"""

        # define the output container
        container: list = []

        # loop through all the arrays with reference keys
        for idx_arr in idx_arrays:
            # conduct error handling to take care of cases where players did not play in postseason
            try:
                container.append({i: idx_arr[i - 1] for i in range(1, len(idx_arr) + 1)})
            except (IndexError, KeyError):
                # add an empty mapping
                container.append({})

        return container

    def _create_player_reference_keys_general(self, response, post_flag: bool = True) -> dict:
        """create the reference keys for the player"""

        # get the player reference keys available for scraping from page
        available_headers: list = response.xpath('//h2/text()').getall()

        # define the reference mapping of keywords
        reference_mapping: dict = self.settings_micro.player_keyword_idx

        # filter the available headers
        available_headers = [item.lower().replace('playing', '') for
                             item in available_headers]
        available_headers = [item for item in available_headers if 'privacy' not in item.lower()]

        # filter based on string similarity
        available_headers = [find_most_similar_key(item, reference_mapping).lower() for
                             item in available_headers]

        # create a sub mapping with all the wanted keys
        return {key: reference_mapping[key] for key in available_headers if key in reference_mapping}

    def _clean_team_links(self, teams_links: list) -> list:
        """clean out the team links only to the valid ones for scraping purposes"""
        return [link for link in teams_links if f"{self.general_sport}/stats" in link]

    @staticmethod
    def _postprocess_data(data: list, header_name: str) -> list:
        """postprocess the data coming from scraper before feeding it to the pipeline"""

        # check for invalid team names
        if header_name == 'team':
            new_data = [i for i in data if 'division' not in i.lower()]
            new_data = [i for i in new_data if 'conference' not in i.lower()]
        else:
            new_data = [i for i in data if not isinstance(i, type(None))]

        return new_data

    def parse(self, response: Any, **kwargs: Any) -> Any:
        """parsing the response coming from the web page"""

        # get the links to follow
        standings_link: str = response.xpath('//p[@class="clear"]/a[1]').attrib['href']
        leaders_link: str = response.xpath('//p[@class="clear"]/a[2]').attrib['href']

        # get all the links to the baseball teams
        teams_links: list = response.xpath("//tbody//tr//td/a/@href").getall()
        teams_links = self._clean_team_links(teams_links)

        # create callback methods for the next page crawler
        yield response.follow(standings_link, callback=self.standings_parse)
        yield response.follow(leaders_link, callback=self.leaders_parse)

        # follow each link in the teams
        for team_link in teams_links:
            yield response.follow(team_link, callback=self.teams_parse)

    def standings_parse(self, response: Any, **kwargs: Any) -> Any:
        """define the standings parser method"""

        # define the output container
        container = NHLStandingsItem()
        container['sport'] = self.sport

        # iterate through the keywords and go through each table on the page
        for idx, keyword_name in enumerate(self.settings_macro.standings_keywords.keys()):
            # iterate through each header in the selected table
            for header_idx, header_name in self.settings_macro.standings_keywords[keyword_name].items():
                # fetch the data
                data = response.xpath(f"(//table[@class='sortable'])[{idx+1}]/tbody//tr//td[{header_idx}]").getall()
                data = self._postprocess_data(data, header_name)

                # add the data to the container
                try:
                    # add data first time
                    container[keyword_name][header_name] = data
                except KeyError:
                    # if section not created add an empty dictionary
                    container[keyword_name] = {}
                    container[keyword_name][header_name] = data

        # get all the team names
        teams_names: list = response.xpath("(//table[@class='logos'])/tbody//tr/td/a//text()").getall()
        container['teams'] = {'names': [i for i in teams_names if 'division' not in i.lower()]}
        container['key'] = str(response.request.url)
        yield container

    def leaders_parse(self, response: Any, **kwargs: Any) -> Any:
        """define the leaders parser method"""

        # define the output container
        container = NHLLeadersItem()
        container['key'] = str(response.request.url)
        container['sport'] = self.sport

        # iterate through each table by keyword
        for idx, keyword_name in enumerate(self.settings_macro.leaders_keyword_idx.keys()):
            # iterate through each header in the table
            for header_idx, header_name in self.settings_macro.leaders_keyword_idx[keyword_name].items():
                # fetch the data
                data = response.xpath(f"(//table[@class='sortable'])[{idx+1}]/tbody//tr/td[{header_idx}]").getall()
                # add the data to the container
                try:
                    # add data first time
                    container[keyword_name][header_name] = data
                except KeyError:
                    # if section not created add an empty dictionary
                    container[keyword_name] = {}
                    container[keyword_name][header_name] = data
        yield container

    def teams_parse(self, response: Any, **kwargs: Any) -> Any:
        """parser for all team specific stats"""
        # define the output container
        container = NHLTeamsItem()

        # get the date of updating
        container['updated']: str = self._updated_date(response)
        container['key'] = str(response.request.url)
        container['sport'] = self.sport

        # define the array of reference keys
        reference_keys = self.settings_micro.keyword_idx_teams

        # iterate through each table
        for idx, keyword_name in enumerate(reference_keys.keys()):
            # iterate through each item
            for key, header_name in reference_keys[keyword_name].items():
                # adjust the xpath expression and fetch the response
                data = response.xpath(f"(//table[@class='sortable'])[{idx+1}]/tbody//tr/td[{key}]").getall()

                # add data to the final container
                try:
                    # add data first time
                    container[keyword_name][header_name] = data
                except KeyError:
                    # if section not created add an empty dictionary
                    container[keyword_name] = {}
                    container[keyword_name][header_name] = data

        # get the link for the team roster and crawl it and YIELD data
        yield container
        roster_link: str = response.xpath("//a[contains(@href, 'roster')]/@href").get()
        yield response.follow(roster_link, callback=self.roster_parse)

    def roster_parse(self, response: Any, **kwargs: Any) -> Any:
        """parse the roster of the team"""

        # define the output container
        container = NHLRosterItem()
        container['roster'] = {}
        container['sport'] = self.sport

        # add the date of most recent update
        container['updated']: str = self._updated_date(response)
        container['key'] = str(response.request.url)

        # get the player links
        players_links: list = response.xpath("//td/a/@href").getall()

        # select all the data and add it to the output container
        for key, value in self.settings_micro.roster_idx.items():
            container['roster'][value] = response.xpath(f"(//table[@class='sortable'])[1]/tbody//tr/"
                                                        f"td[{key}]").getall()

        # crawl each next player profile and YIELD data
        yield container

        for players_link in players_links:
            yield response.follow(players_link, callback=self.players_parse)

    def _modes(self, response: Any) -> list:
        """compute the modes"""

        # query for the modes
        modes: list = response.xpath("(//h2/text()) | (//tr//a/text()) | (//th/text())").getall()

        # make all the modes lowercase
        modes = [x.lower().replace('.', '') for x in modes if 'privacy' not in x.lower()]

        # process the modes
        modes = [i.replace('/', '_').replace(' %', '_perc').replace('%', '_perc').replace('+', 'pos'
                            ).replace('-', 'neg') for i in modes]
        modes = [i.replace('pts', 'points') for i in modes]

        return modes

    def _filter_reference_keys(self, modes: list, reference_keys: dict):
        """filter the reference keys for the player parsing function"""

        # loop through each of the reference keys
        new_dict: dict = {}
        for key, values in reference_keys.items():
            new_mapping: dict = {player_key: value for player_key, value in values.items() if value in modes}

            # reorder the indices of dictionary
            output_mapping: dict = {}
            for key_id, new_key in zip(new_mapping, [i for i in range(1, len(new_mapping) + 1)]):
                output_mapping[new_key] = new_mapping[key_id]

            new_dict[key] = output_mapping

        return new_dict

    def players_parse(self, response: Any, **kwargs: Any) -> Any:
        """parse each player's profile"""

        # define the output container
        container = NHLPlayerItem()
        container['sport'] = self.sport

        # create the reference keys used later to scrape the player stats by section
        reference_keys: dict = self._create_player_reference_keys_general(response)
        print('original reference keys: ', reference_keys)
        print('\n')

        # compute the modes and filter the reference keys
        modes: list = self._modes(response)
        print('modes: ', modes)
        print('\n')
        reference_keys = self._filter_reference_keys(modes, reference_keys)
        print('processed reference keys: ', reference_keys)
        print('\n')

        # get the player descriptions
        player_descriptions: list = response.xpath("//div[@class='content']//p//text()").getall()

        # add the player descriptions to the output container and filter for no empty spaces
        container['description']: list = player_descriptions
        container['key'] = str(response.request.url)

        # iterate through each table using the idx integer valued
        for table_idx, keyword_name in enumerate(reference_keys.keys()):
            # iterate through each of the header names in the player keywords
            for header_idx, header_name in reference_keys[keyword_name].items():
                # check if the mode and header line up
                if header_name not in modes:
                    container[keyword_name][header_name] = []
                    continue
                # get the data from the selector
                data: list = response.xpath(f"(//table[@class='sortable'])[{table_idx+1}]"
                                            f"/tbody//tr/td[{header_idx}]").getall()

                # add the data to the output container
                try:
                    # add data first time
                    container[keyword_name][header_name] = data
                except KeyError:
                    # if section not created add an empty dictionary
                    container[keyword_name] = {}
                    container[keyword_name][header_name] = data

        yield container