import scrapy
from typing import Any, List
from datetime import datetime
from pydantic import BaseModel
from ..items import MLBStandingsItem, MLBLeadersItem, MLBTeamsItem, MLBPlayerItem, MLBRosterItem
from .models import find_most_similar_key


class KeywordGeneral(BaseModel):
    """general header names for whole site to avoid local errors"""

    # pitching mappings
    pitching: list = ['players', 'w', 'l', 'w_perc', 'era', 'g', 'gs', 'gf', 'cg', 'sho', 'sv', 'ip','h', 'r','er','hr',
                      'bb', 'ibb', 'so', 'hbp', 'bk', 'wp', 'bf']
    batting: list = ['players', 'gp', 'pa', 'ab', 'r', 'h', '2b', '3b', 'hr', 'rbi', 'sb', 'cs', 'bb', 'so', 'ba',
                     'obp', 'slg', 'ops', 'tb', 'dp', 'hbp', 'sh', 'sf', 'ibb']
    fielding: list = ['players', 'position', 'gp', 'gs', 'inn', 'po', 'a', 'e', 'dp', 'fld_perc', 'rf_9', 'rf_g', 'pb',
                      'wp', 'sb', 'cs', 'cs_perc']
    roster: list = ['player', 'position', 'bats', 'throws', 'birth_date', 'height', 'weight', 'origin']
    player_pitching: list = ['year', 'league', 'team', 'w', 'l', 'w_perc', 'era', 'g', 'gs', 'gf', 'cg', 'sho', 'sv',
                             'ip', 'h','r', 'er', 'hr', 'bb', 'ibb', 'so', 'hbp', 'bk', 'wp', 'bf']
    player_batting: list = ['year', 'league', 'team', 'gp', 'pa', 'ab', 'r', 'h', '2b', '3b', 'hr', 'rbi', 'sb', 'cs',
                            'bb', 'so', 'ba', 'obp', 'slg', 'ops', 'tb', 'dp', 'hbp', 'sh', 'sf', 'ibb']
    player_fielding: list = ['year', 'league', 'team', 'position', 'gp', 'gs', 'inning', 'po', 'a', 'e','dp','fld_perc',
                             'rf_9', 'rf_g']
    regular_season: list = ['team', 'w', 'l', 'r', 'ra']
    hits: list = ['player', 'team', 'hits', '2b', '3b', 'hr']
    home_runs: list = ['player', 'team', 'hr']
    doubles: list = ['player', 'team', '2b']
    triples: list = ['player', 'team', '3b']
    rbi: list = ['player', 'team', 'rbi']
    wins: list = ['player', 'team', 'w', 'l']
    shutouts: list = ['player', 'team', 'sho']
    strikeouts: list = ['player', 'team', 'so']
    saves: list = ['player', 'team', 'saves']


general_model = KeywordGeneral()


# scraping key base models
class ScrapingKeysMicro(BaseModel):
    """scraping keys for the scraping process hardcoded"""

    # pitching mappings
    teams_pitching_idx: dict = {i: general_model.pitching[i-1] for i in range(1, len(general_model.pitching) + 1)}

    # batting mappings
    teams_batting_idx: dict = {i: general_model.batting[i - 1] for i in range(1, len(general_model.batting) + 1)}

    # fielding mappings
    teams_fielding_idx: dict = {i: general_model.fielding[i - 1] for i in range(1, len(general_model.fielding) + 1)}

    # map it to each keyword in the output headers
    keyword_idx_teams: dict = {
        'pitching': teams_pitching_idx,
        'batting': teams_batting_idx,
        'fielding': teams_fielding_idx
    }

    # roster mappings
    roster_idx: dict = {i: general_model.roster[i - 1].lower() for i in range(1, len(general_model.roster) + 1)}

    # player level statistics
    # pitching
    player_pitching_idx: dict = {i: general_model.player_pitching[i - 1] for i in range(1,
                                                                            len(general_model.player_pitching) + 1)}

    # batting
    player_batting_idx: dict = {i: general_model.player_batting[i - 1] for i in range(1,
                                                                                len(general_model.player_batting) + 1)}

    # fielding
    player_fielding_idx: dict = {i: general_model.player_fielding[i - 1] for i in range(1,
                                                                                len(general_model.player_fielding) + 1)}
    player_keyword_idx: dict = {
        'pitching': player_pitching_idx,
        'batting': player_batting_idx,
        'fielding': player_fielding_idx
    }


class ScrapingKeysMacro(BaseModel):
    """scraping keys and mappings for the standings and leaders macro level stats"""

    # standings mapping
    # regular season
    regular_season_idx: dict = {i: general_model.regular_season[i - 1] for i in range(1,
                                                                                len(general_model.regular_season) + 1)}
    # define the general keyword mappings
    standings_keywords: dict = {
        'american_league': regular_season_idx,
        'national_league': regular_season_idx
    }

    # leaders mappings
    # hits leaders
    hits_idx: dict = {i: general_model.hits[i - 1] for i in range(1, len(general_model.hits) + 1)}

    # home runs
    home_runs_idx: dict = {i: general_model.home_runs[i - 1] for i in range(1, len(general_model.home_runs) + 1)}

    # doubles leaders
    doubles_idx: dict = {i: general_model.doubles[i - 1] for i in range(1, len(general_model.doubles) + 1)}

    # triples leaders
    triples_idx: dict = {i: general_model.triples[i - 1] for i in range(1, len(general_model.triples) + 1)}

    # rbi leaders
    rbi_idx: dict = {i: general_model.rbi[i - 1] for i in range(1, len(general_model.rbi) + 1)}

    # wins leaders
    wins_idx: dict = {i: general_model.wins[i - 1] for i in range(1, len(general_model.wins) + 1)}

    # shutout leaders
    shutouts_idx: dict = {i: general_model.shutouts[i - 1] for i in range(1, len(general_model.shutouts) + 1)}

    # strikeout leaders
    strikeouts_idx: dict = {i: general_model.strikeouts[i - 1] for i in range(1, len(general_model.strikeouts) + 1)}

    # save leaders
    saves_idx: dict = {i: general_model.saves[i - 1] for i in range(1, len(general_model.saves) + 1)}

    # team leaders stats keywords
    leaders_keyword_idx: dict = {
        'hits': hits_idx,
        'home_runs': home_runs_idx,
        'doubles': doubles_idx,
        'triples': triples_idx,
        'rbi': rbi_idx,
        'wins': wins_idx,
        'shutouts': shutouts_idx,
        'strikeouts': strikeouts_idx,
        'saves': saves_idx
    }


class MatchMLBScraper(scrapy.Spider):
    """match level scraper for the mlb league in USA"""

    # class variables for naming and starting domain
    name: str = 'mlb_matches'
    sport: str = 'mlb'
    start_urls: List[str] = ['https://www.statscrew.com/baseball/y-2023']

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        # initiate the settings models with the mappings and keyword settings
        self.settings_micro = ScrapingKeysMicro()
        self.settings_macro = ScrapingKeysMacro()

        # roster links storage
        self.pitcher_status_key: str = 'pitching:'
        self.roster_links: list = []

    def parse(self, response: Any, **kwargs: Any) -> Any:
        """parsing the response coming from the web page"""

        # get the links to follow
        standings_link: str = response.xpath('//p[@class="clear"]/a[1]').attrib['href']
        leaders_link: str = response.xpath('//p[@class="clear"]/a[2]').attrib['href']

        # get all the links to the baseball teams
        teams_links: list = response.xpath("//p/a[contains(@href, 'baseball/stats')]//@href").getall()

        # create callback methods for the next page crawler
        yield response.follow(standings_link, callback=self.standings_parse)
        yield response.follow(leaders_link, callback=self.leaders_parse)

        # follow each link in the teams
        for team_link in teams_links:
            yield response.follow(team_link, callback=self.teams_parse)

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
    def _check_pitcher_status(html_expression: list, search_key: str) -> bool:
        """check whether a player is a pitcher or not from their player profile page"""

        # build the player summary string to check for string expressions
        summary_str: str = ' '.join([str(html).lower() for html in html_expression])

        # check if the search key is in the string
        if search_key in summary_str:
            return True
        else:
            return False

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
        available_headers = [find_most_similar_key(item, reference_mapping) for
                             item in available_headers]

        # create a sub mapping with all the wanted keys
        return {key: reference_mapping[key] for key in available_headers if key in reference_mapping}

    def standings_parse(self, response: Any, **kwargs: Any) -> Any:
        """define the standings parser method"""

        # define the output container
        container = MLBStandingsItem()
        container['key'] = str(response.request.url)
        container['sport'] = self.sport

        # iterate through the keywords and go through each table on the page
        for idx, keyword_name in enumerate(self.settings_macro.standings_keywords.keys()):
            # iterate through each header in the selected table
            for header_idx, header_name in self.settings_macro.standings_keywords[keyword_name].items():
                # fetch the data
                data = response.xpath(f"(//table[@class='sortable'])[{idx+1}]/tbody//tr//td[{header_idx}]").getall()

                # check for invalid team names
                if header_name == 'Team':
                    data = [i for i in data if 'division' not in i.lower()]

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
        yield container

    def leaders_parse(self, response: Any, **kwargs: Any) -> Any:
        """define the leaders parser method"""

        # define the output container
        container = MLBLeadersItem()
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
        container = MLBTeamsItem()

        # get the date of updating
        container['updated']: str = self._updated_date(response)
        container['key'] = str(response.request.url)
        container['sport'] = self.sport

        # iterate through each table
        for idx, keyword_name in enumerate(self.settings_micro.keyword_idx_teams.keys()):
            # iterate through each item
            for key, header_name in self.settings_micro.keyword_idx_teams[keyword_name].items():
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
        container = MLBRosterItem()
        container['roster'] = {}
        container['sport'] = self.sport

        # add the date of most recent update
        container['updated']: str = self._updated_date(response)
        container['key'] = str(response.request.url)

        # get the player links
        players_links: list = response.xpath("(//table[@class='sortable'])[1]/tbody/tr/td[1]/a/@href").getall()

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
            for key_id, new_key in zip(new_mapping, [i for i in range(1, len(new_mapping)+1)]):
                output_mapping[new_key] = new_mapping[key_id]

            new_dict[key] = output_mapping

        return new_dict

    def players_parse(self, response: Any, **kwargs: Any) -> Any:
        """parse each player's profile"""

        # define the output container
        container = MLBPlayerItem()
        container['sport'] = self.sport

        # check the type of the player (pitcher vs. non pitcher)
        reference_keys: dict = self._create_player_reference_keys_general(response)
        print('original reference keys: ', reference_keys)

        modes = self._modes(response)
        print('these are the modes: ', modes)

        reference_keys = self._filter_reference_keys(modes, reference_keys)
        print('filtered reference keys: ', reference_keys)

        # get the player descriptions
        player_descriptions: list = response.xpath("//div[@class='content']//p//text()").getall()

        # add the player descriptions to the output container
        container['description']: list = player_descriptions
        container['key'] = str(response.request.url)

        # iterate through each table using the idx integer valued
        for table_idx, keyword_name in enumerate(reference_keys.keys()):
            # iterate through each of the header names in the player keywords
            for header_idx, header_name in reference_keys[keyword_name].items():
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