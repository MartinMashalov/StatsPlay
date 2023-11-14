import scrapy
from typing import Any, List
from pydantic import BaseModel
from pprint import pprint
from datetime import datetime
from ..items import NBAStandingsItem, NBALeadersItem, NBATeamsItem, NBAPlayerItem, NBARosterItem


class KeywordGeneral(BaseModel):
    """general header names for whole site to avoid local errors"""

    # team stats
    totals: list = ["player", "position", "gp", "min", "fga", "fgm", "3pa", "3pm", "fta", "ftm", "off", "def", "reb",
                    "ast", "stl", "blk", "to", "pf", "points"]
    averages: list = ["player", "position", "gp", "mpg", "fg_perc", "3p_perc", "ft_perc", "off", "def", "reb", "ast",
                      "stl", "blk", "to", "pf", "points"]
    postseason_totals: list = ["player", "position", "gp", "min", "fga", "fgm", "3pa", "3pm", "fta", "ftm", "off",
                               "def", "reb", "ast", "stl", "blk", "to", "pf", "points"]
    postseason_averages: list = ["player", "position", "gp", "mpg", "fg_perc", "3p_perc", "ft_perc", "off", "def",
                                 "reb", "ast", "stl", "blk", "to", "pf", "points"]
    # roster statistics
    roster: list = ['player', 'position', 'birth_date', 'height', 'weight', 'college', 'origin']
    # player level stats
    player_totals: list = ["year", "team", "gp", "gs", "min", "fga", "fgm", "3pa", "3pm", "fta", "ftm", "off",
                           "def", "reb", "ast", "stl", "blk", "to", "pf", "pt"]
    player_averages: list = ["year", "team", "gp", "gs", "mpg", "fg_perc", "3p_perc", "ft_perc", "off", "def", "reb",
                             "ast", "stl", "blk", "to", "pf", "points"]
    player_post_totals: list = ["year", "team", "gp", "min", "fga", "fgm", "3pa", "3pm", "fta", "ftm", "off", "def",
                                "reb", "ast", "stl", "blk", "to", "pf", "points"]
    player_post_averages: list = ["year", "team", "gp", "mpg", "fg_perc", "3p_perc", "ft_perc", "off", "def", "reb",
                                  "ast", "stl", "blk", "to", "pf", "points"]
    regular_season: list = ['team', 'w', 'l', 'pct', 'pf', 'pa']
    points: list = ['player', 'team', 'points', 'fg', '3pt', 'ft']
    rebounds: list = ['player', 'team', 'reb', 'offense', 'defense']
    assists: list = ['player', 'team', 'assist']
    steals: list = ['player', 'team', 'steals']
    blocks: list = ['player', 'team', 'blocks']


general_model = KeywordGeneral()


# scraping key base models
class ScrapingKeysMicro(BaseModel):
    """scraping keys for the scraping process hardcoded"""

    # totals  mappings
    totals_idx: dict = {i: general_model.totals[i-1] for i in range(1, len(general_model.totals) + 1)}

    # averages  mappings
    averages_idx: dict = {i: general_model.averages[i - 1] for i in range(1, len(general_model.averages) + 1)}

    # postseason totals mappings
    post_totals_idx: dict = {i: general_model.postseason_totals[i - 1] for i in range(1,
                                                                            len(general_model.postseason_totals) + 1)}

    # postseason averages mappings
    post_averages_idx: dict = {i: general_model.postseason_averages[i - 1] for i in range(1,
                                                                            len(general_model.postseason_averages) + 1)}

    # map it to each keyword in the output headers
    keyword_idx_teams_postseason: dict = {
        'totals': totals_idx,
        'averages': averages_idx,
        'postseason_totals': post_totals_idx,
        'postseason_averages': post_averages_idx
    }

    keyword_idx_teams_non_postseason: dict = {
        'totals': totals_idx,
        'averages': averages_idx
    }

    # roster mappings
    roster_idx: dict = {i: general_model.roster[i - 1].lower() for i in range(1, len(general_model.roster) + 1)}

    # player level statistics
    # totals
    player_totals_idx: dict = {i: general_model.player_totals[i - 1] for i in range(1,
                                                                            len(general_model.player_totals) + 1)}

    # averages
    player_averages_idx: dict = {i: general_model.player_averages[i - 1] for i in range(1,
                                                                                len(general_model.player_averages) + 1)}

    # post season totals
    player_post_totals_idx: dict = {i: general_model.player_post_totals[i - 1] for i in range(1,
                                                                        len(general_model.player_post_totals) + 1)}

    # postseason averages
    player_post_averages_idx: dict = {i: general_model.player_post_averages[i - 1] for i in range(1,
                                                                        len(general_model.player_post_averages) + 1)}

    # create the overall player keyword indices
    player_keyword_idx_postseason: dict = {
        'totals': player_totals_idx,
        'averages': player_averages_idx,
        'post_totals': player_post_totals_idx,
        'post_averages': player_post_averages_idx
    }

    player_keyword_idx_non_postseason: dict = {
        'totals': player_totals_idx,
        'averages': player_averages_idx
    }


class ScrapingKeysMacro(BaseModel):
    """scraping keys and mappings for the standings and leaders macro level stats"""

    # standings mapping
    # regular season
    regular_season_idx: dict = {i: general_model.regular_season[i - 1] for i in range(1,
                                                                                len(general_model.regular_season) + 1)}
    # define the general keyword mappings
    standings_keywords: dict = {
        'NBA': regular_season_idx,
    }

    # leaders mappings
    # hits leaders
    points_idx: dict = {i: general_model.points[i - 1] for i in range(1, len(general_model.points) + 1)}

    # home runs
    rebounds_idx: dict = {i: general_model.rebounds[i - 1] for i in range(1, len(general_model.rebounds) + 1)}

    # doubles leaders
    assists_idx: dict = {i: general_model.assists[i - 1] for i in range(1, len(general_model.assists) + 1)}

    # triples leaders
    steals_idx: dict = {i: general_model.steals[i - 1] for i in range(1, len(general_model.steals) + 1)}

    # rbi leaders
    blocks_idx: dict = {i: general_model.blocks[i - 1] for i in range(1, len(general_model.blocks) + 1)}

    # team leaders stats keywords
    leaders_keyword_idx: dict = {
        'points': points_idx,
        'rebounds': rebounds_idx,
        'assists': assists_idx,
        'steals': steals_idx,
        'blocks': blocks_idx
    }


class MatchNBAScraper(scrapy.Spider):
    """match level scraper for the mlb league in USA"""

    # class variables for naming and starting domain
    name: str = 'nba_matches'
    sport: str = 'nba'
    general_sport: str = 'basketball'
    start_urls: List[str] = ['https://www.statscrew.com/basketball/l-NBA/y-2022']

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        # initiate the settings models with the mappings and keyword settings
        self.settings_micro = ScrapingKeysMicro()
        self.settings_macro = ScrapingKeysMacro()

        # roster links storage
        self.roster_links: list = []

    @staticmethod
    def _check_postseason_presence(html_expression: list, search_key: str) -> bool:
        """check if team or player has had postseason presence on their profile"""

        # build the entity summary string to check for string expressions
        summary_str: str = ' '.join([str(html).lower() for html in html_expression])

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

    @staticmethod
    def _process_player_keys_arr(arr: list) -> list:
        """process the player array to edit the keys to the conventions"""

        # edit the array to change the position and points item
        arr = ['team' if x.lower() == 'pos' else x for x in arr]
        arr = ['points' if x.lower() == 'pts' else x for x in arr]
        # make everything lowercase
        arr = [x.lower() for x in arr]
        return arr

    def _create_player_reference_keys_general(self, response, post_flag: bool = True):
        """create the reference keys for the player"""

        # get the totals reference keys
        totals: list = response.xpath("(//div[@class='stat-table'])[2]//th/a//text()").getall()
        totals = self._process_player_keys_arr(['year', *totals])
        # get the averages reference keys
        averages: list = response.xpath("(//div[@class='stat-table'])[3]//th/a//text()").getall()
        averages = self._process_player_keys_arr(['year', *averages])
        # get the totals reference keys
        postseason_totals: list = response.xpath("(//div[@class='stat-table'])[4]//th/a//text()").getall()
        postseason_totals = self._process_player_keys_arr(['year', *postseason_totals])
        postseason_averages: list = response.xpath("(//div[@class='stat-table'])[5]//th/a//text()").getall()
        postseason_averages = self._process_player_keys_arr(['year', *postseason_averages])
        # create the indices with the player keys dynamically with error handling
        player_totals_idx, player_averages_idx, player_post_totals_idx, player_post_averages_idx = self._create_idx(
            [totals, averages, postseason_totals, postseason_averages]
        )

        # check the flag for the type of player statistics (post season included)
        return {
                'totals': player_totals_idx,
                'averages': player_averages_idx,
                'post_totals': player_post_totals_idx,
                'post_averages': player_post_averages_idx
            } if post_flag else {
                'totals': player_totals_idx,
                'averages': player_averages_idx
            }

    @staticmethod
    def _postprocess_data(data: list, header_name: str) -> list:
        """postprocess the data coming from scraper before feeding it to the pipeline"""

        # check for invalid team names
        if header_name == 'team' or header_name == 'Team':
            data = [i for i in data if 'division' not in i.lower()]
            data = [i for i in data if 'conference' not in i.lower()]
        else:
            data = [i for i in data if not isinstance(i, type(None))]

        return data

    def _clean_team_links(self, teams_links: list) -> list:
        """clean out the team links only to the valid ones for scraping purposes"""
        return [link for link in teams_links if f"{self.general_sport}/stats" in link]

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
        container = NBAStandingsItem()
        container['sport'] = self.sport

        # iterate through the keywords and go through each table on the page
        for idx, keyword_name in enumerate(self.settings_macro.standings_keywords.keys()):
            # iterate through each header in the selected table
            for header_idx, header_name in self.settings_macro.standings_keywords[keyword_name].items():
                # fetch the data
                data = response.xpath(f"(//table[@class='sortable'])[{idx+1}]/tbody//tr//td[{header_idx}]").getall()
                data = self._postprocess_data(data, header_name)

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
        container['key'] = str(response.request.url)
        yield container

    def leaders_parse(self, response: Any, **kwargs: Any) -> Any:
        """define the leaders parser method"""

        # define the output container
        container = NBALeadersItem()
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
        container = NBATeamsItem()

        # get the date of updating
        container['updated']: str = self._updated_date(response)
        container['key'] = str(response.request.url)
        container['sport'] = self.sport

        # check if postseason results should be scraped
        if self._check_postseason_presence(response.xpath('//h2/text()').getall(), 'postseason'):
            reference_keys: dict = self.settings_micro.keyword_idx_teams_postseason
        else:
            reference_keys: dict = self.settings_micro.keyword_idx_teams_non_postseason

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
        container = NBARosterItem()
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
            for key_id, new_key in zip(new_mapping, [i for i in range(1, len(new_mapping) + 1)]):
                output_mapping[new_key] = new_mapping[key_id]

            new_dict[key] = output_mapping

        return new_dict

    def players_parse(self, response: Any, **kwargs: Any) -> Any:
        """parse each player's profile"""

        # define the output container
        container = NBAPlayerItem()
        container['sport'] = self.sport

        # check if postseason results should be scraped
        post_flag: bool = self._check_postseason_presence(response.xpath('//h2/text()').getall(), 'postseason')
        reference_keys: dict = self._create_player_reference_keys_general(response, post_flag=post_flag)

        # compute the modes
        modes: list = self._modes(response)
        reference_keys = self._filter_reference_keys(modes, reference_keys)

        # get the player descriptions
        player_descriptions: list = response.xpath("//div[@class='content']//p//text()").getall()

        # add the player descriptions to the output container
        container['description']: list = player_descriptions
        container['key'] = str(response.request.url)

        # iterate through each table using the idx integer valued
        for table_idx, keyword_name in enumerate(reference_keys.keys()):
            # iterate through each of the header names in the player keywords
            for header_idx, header_name in reference_keys[keyword_name].items():
                # check if the mode and header line up
                if header_name not in modes and header_name != 'points':
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