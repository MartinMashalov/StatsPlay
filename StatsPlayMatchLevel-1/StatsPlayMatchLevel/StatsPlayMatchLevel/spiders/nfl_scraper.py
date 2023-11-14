"""scraper for NFL matches"""

import scrapy
from typing import Any, List
from pydantic import BaseModel
from pprint import pprint
from datetime import datetime
from ..items import NFLStandingsItem, NFLLeadersItem, NFLTeamsItem, NFLPlayerItem, NFLRosterItem
from .models import find_most_similar_key


class KeywordGeneral(BaseModel):
    """general header names for whole site to avoid local errors"""

    # team stats
    team_passing: list = ["player", "att", "comp", "comp_perc", "yds", "yds_att", "tds", "td_perc", "ints", "int_perc",
                          "long", "sacked", "yds lost", "rating"]
    team_rushing: list = ["player", "no", "yds", "avg", "long", "tds"]
    team_receiving: list = ["player", "no", "yds", "avg", "long", "tds"]
    team_kicking: list = ["player", "x_ca", "x_cm", "x_cp_perc", "fga", "fgm", "fg_perc", "points"]
    team_punting: list = ["player", "no", "yds", "avg", "long"]
    team_punts_returned: list = ["player", "no", "yds", "avg", "long", "tds"]
    team_kicks_returned: list = ["player", "no", "yds", "avg", "long", "tds"]
    team_interceptions: list = ["player", "no", "yds", "avg", "long", "tds"]
    team_sacks: list = ["player", "no", "yds", "avg"]
    team_defense: list = ["player", "tackle", "solo", "ast", "brup", "fum", "frec", "fyds", "ftd", "ff"]
    team_points: list = ["player", "rush", "rec", "punt", "kick", "mfg", "int", "fum", "other", "fg", "x_c", "single",
                         "2pt", "saf", "points"]
    # roster
    roster: list = ["number", "player", "position", "birth_date", "height", "weight", "college", "origin", "gp"]
    # player stats
    player_passing: list = ["year", "team", "att", "comp", "comp_perc", "yds", "yds_att", "tds", "td_perc",
                            "ints","int_perc","long", "rating"]
    player_rushing: list = ["year", "team", "no", "yds", "avg", "long", "tds"]
    player_receiving: list = ["year", "team", "no", "yds", "avg", "long", "tds"]
    player_kicking: list = ["year", "team", "x_ca", "x_cm", "x_cp_perc", "fga", "fgm", "fg_perc", "ko", "yds", "avg",
                            "long", "kos", "points"]
    player_punting: list = ["year", "team", "no", "yds", "avg", "long"]
    player_punts_returned: list = ["year", "team", "no", "yds", "avg", "long", "tds"]
    player_kicks_returned: list = ["year", "team", "no", "yds", "avg", "long", "tds"]
    player_interceptions: list = ["year", "team", "no", "yds", "avg", "long", "tds"]
    player_sacks: list = ["year", "team", "no", "yds", "avg"]
    player_defense: list = ["year", "team", "tackle", "solo", "ast", "tfl", "tfly", "brup", "pd", "qbh", "fum", "f_rec",
                            "fyds", "ftd", "ff"]
    player_points: list = ["year", "team", "rush", "rec", "punt", "kick", "mfg", "int", "fum", "other", "fg", "x_c",
                           "single", "2pt", "saf", "points"]
    player_career: list = ["year", "league", "team", "gp", "gs"]

    # standings
    regular_season: list = ['team', 'w', 'l', 't', 'pct', 'pf', 'pa']
    # statistical leaders
    passing_touchdowns: list = ["player", "team", "td", "att", "comp", "yds", "int"]
    passing_yardage: list = ["player", "team", "td", "att", "comp", "yds", "int"]
    rushing_yardage: list = ["player", "team", "yds", "td", "long"]
    rushing_touchdowns: list = ["player", "team", "td", "yds", "long"]
    receiving: list = ["player", "team", "rec", "yds", "td", "long"]
    receiving_yards: list = ["player", "team", "yds", "rec", "td", "long"]
    receiving_touchdowns: list = ["player", "team", "td", "rec", "yds", "long"]
    interceptions: list = ["player", "team", "int", "yds", "td", "long"]
    sacks: list = ["player", "team", "sacks", "yds", "avg"]


general_model = KeywordGeneral()


# scraping key base models
class ScrapingKeysMicro(BaseModel):
    """scraping keys for the scraping process hardcoded"""

    # totals  mappings
    passing_idx: dict = {i: general_model.team_passing[i-1] for i in range(1, len(general_model.team_passing) + 1)}
    rushing_idx: dict = {i: general_model.team_rushing[i - 1] for i in range(1, len(general_model.team_rushing) + 1)}
    receiving_idx: dict = {i: general_model.team_receiving[i - 1] for i in
                           range(1, len(general_model.team_receiving) + 1)}
    kicking_idx: dict = {i: general_model.team_kicking[i - 1] for i in range(1, len(general_model.team_kicking) + 1)}
    punting_idx: dict = {i: general_model.team_punting[i - 1] for i in range(1, len(general_model.team_punting) + 1)}
    punts_returned_idx: dict = {i: general_model.team_punts_returned[i - 1] for i in
                                range(1, len(general_model.team_punts_returned) + 1)}
    kicks_returned_idx: dict = {i: general_model.team_kicks_returned[i - 1] for i in
                                range(1, len(general_model.team_kicks_returned) + 1)}
    interceptions_idx: dict = {i: general_model.team_interceptions[i - 1] for i in
                               range(1, len(general_model.team_interceptions) + 1)}
    sacks_idx: dict = {i: general_model.team_sacks[i - 1] for i in range(1, len(general_model.team_sacks) + 1)}
    defense_idx: dict = {i: general_model.team_defense[i - 1] for i in range(1, len(general_model.team_defense) + 1)}
    points_idx: dict = {i: general_model.team_points[i - 1] for i in range(1, len(general_model.team_points) + 1)}

    # map it to each keyword in the output headers
    keyword_idx_teams: dict = {
        'passing': passing_idx,
        'rushing': rushing_idx,
        'receiving': receiving_idx,
        'kicking': kicking_idx,
        'punting': punting_idx,
        'punts_returned': punts_returned_idx,
        'kicks_returned': kicks_returned_idx,
        'interceptions': interceptions_idx,
        'sacks': sacks_idx,
        'defense': defense_idx,
        'points': points_idx,
    }

    # roster mappings
    roster_idx: dict = {i: general_model.roster[i - 1].lower() for i in range(1, len(general_model.roster) + 1)}

    # player level statistics
    player_passing_idx: dict = {i: general_model.player_passing[i - 1] for i in
                                range(1, len(general_model.player_passing) + 1)}
    player_rushing_idx: dict = {i: general_model.player_rushing[i - 1] for i in
                                range(1, len(general_model.player_rushing) + 1)}
    player_receiving_idx: dict = {i: general_model.player_receiving[i - 1] for i in
                                  range(1, len(general_model.player_receiving) + 1)}
    player_kicking_idx: dict = {i: general_model.player_kicking[i - 1] for i in
                                range(1, len(general_model.player_kicking) + 1)}
    player_punting_idx: dict = {i: general_model.player_punting[i - 1] for i in
                                range(1, len(general_model.player_punting) + 1)}
    player_punts_returned_idx: dict = {i: general_model.player_punts_returned[i - 1] for i in
                                       range(1, len(general_model.player_punts_returned) + 1)}
    player_kicks_returned_idx: dict = {i: general_model.player_kicks_returned[i - 1] for i in
                                       range(1, len(general_model.player_kicks_returned) + 1)}
    player_interceptions_idx: dict = {i: general_model.player_interceptions[i - 1] for i in
                                      range(1, len(general_model.player_interceptions) + 1)}
    player_sacks_idx: dict = {i: general_model.player_sacks[i - 1] for i in
                              range(1, len(general_model.player_sacks) + 1)}
    player_defense_idx: dict = {i: general_model.player_defense[i - 1] for i in
                                range(1, len(general_model.player_defense) + 1)}
    player_points_idx: dict = {i: general_model.player_points[i - 1] for i in
                               range(1, len(general_model.player_points) + 1)}
    player_career_idx: dict = {i: general_model.player_career[i - 1] for i in
                               range(1, len(general_model.player_career) + 1)}

    # create the overall player keyword indices
    player_keyword_idx: dict = {
        'passing': player_passing_idx,
        'rushing': player_rushing_idx,
        'receiving': player_receiving_idx,
        'kicking': player_kicking_idx,
        'punting': player_punting_idx,
        'punts_returned': player_punts_returned_idx,
        'kicks_returned': player_kicks_returned_idx,
        'interceptions': player_interceptions_idx,
        'sacks': player_sacks_idx,
        'defense': player_defense_idx,
        'scoring': player_points_idx,
        'career': player_career_idx
    }


class ScrapingKeysMacro(BaseModel):
    """scraping keys and mappings for the standings and leaders macro level stats"""

    # standings mapping
    regular_season_idx: dict = {i: general_model.regular_season[i - 1] for i in range(1,
                                                                                len(general_model.regular_season) + 1)}
    # define the general keyword mappings
    standings_keywords: dict = {
        'NFL': regular_season_idx,
    }

    # leaders mappings
    passing_touchdowns_idx: dict = {i: general_model.passing_touchdowns[i - 1] for i in
                                    range(1, len(general_model.passing_touchdowns) + 1)}
    passing_yardage_idx: dict = {i: general_model.passing_yardage[i - 1] for i in
                                 range(1, len(general_model.passing_yardage) + 1)}
    rushing_yardage_idx: dict = {i: general_model.rushing_yardage[i - 1] for i in
                                 range(1, len(general_model.rushing_yardage) + 1)}
    rushing_touchdowns_idx: dict = {i: general_model.rushing_touchdowns[i - 1] for i in
                                    range(1, len(general_model.rushing_touchdowns) + 1)}
    receiving_idx: dict = {i: general_model.receiving[i - 1] for i in range(1, len(general_model.receiving) + 1)}
    receiving_yards_idx: dict = {i: general_model.receiving_yards[i - 1] for i in
                                 range(1, len(general_model.receiving_yards) + 1)}
    receiving_touchdowns_idx: dict = {i: general_model.receiving_touchdowns[i - 1] for i in
                                      range(1, len(general_model.receiving_touchdowns) + 1)}
    interceptions_idx: dict = {i: general_model.interceptions[i - 1] for i in
                               range(1, len(general_model.interceptions) + 1)}
    sacks_idx: dict = {i: general_model.sacks[i - 1] for i in range(1, len(general_model.sacks) + 1)}

    # team leaders stats keywords
    leaders_keyword_idx: dict = {
        'passing_touchdowns': passing_touchdowns_idx,
        'passing_yardage': passing_yardage_idx,
        'rushing_yardage': rushing_yardage_idx,
        'rushing_touchdowns': rushing_touchdowns_idx,
        'receiving': receiving_idx,
        'receiving_yards': receiving_yards_idx,
        'receiving_touchdowns': receiving_touchdowns_idx,
        'interceptions': interceptions_idx,
        'sacks': sacks_idx
    }


class MatchNFLScraper(scrapy.Spider):
    """match level scraper for the mlb league in USA"""

    # class variables for naming and starting domain
    sport: str = 'nfl'
    general_sport: str = 'football'
    name: str = f'{sport}_matches'
    start_urls: List[str] = ['https://www.statscrew.com/football/l-NFL/y-2023']

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
        print('original available headers')
        print(available_headers)

        # define the reference mapping of keywords
        reference_mapping: dict = self.settings_micro.player_keyword_idx

        # filter the available headers
        available_headers = [item.lower().replace('playing', '').replace(' and fumbles', '') for
                             item in available_headers]
        available_headers = [item for item in available_headers if 'privacy' not in item.lower()]
        print(reference_mapping)
        print('available headers')
        pprint(available_headers)
        print('\n')

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
        if header_name == 'Team' or header_name == 'team':
            data = [i for i in data if 'division' not in i.lower()]
            data = [i for i in data if 'conference' not in i.lower()]
        else:
            data = [i for i in data if not isinstance(i, type(None))]

        return data

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
        container = NFLStandingsItem()
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
        container = NFLLeadersItem()
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
        container = NFLTeamsItem()

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
        container = NFLRosterItem()
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
            for key_id, new_key in zip(new_mapping, [i for i in range(1, len(new_mapping)+1)]):
                output_mapping[new_key] = new_mapping[key_id]

            new_dict[key] = output_mapping

        return new_dict

    def players_parse(self, response: Any, **kwargs: Any) -> Any:
        """parse each player's profile"""

        # define the output container
        container = NFLPlayerItem()
        container['sport'] = self.sport

        # create the reference keys used later to scrape the player stats by section
        reference_keys: dict = self._create_player_reference_keys_general(response)

        # compute the modes
        modes: list = self._modes(response)

        # filter out the reference keys for the player
        reference_keys = self._filter_reference_keys(modes, reference_keys)

        # get the player descriptions
        player_descriptions: list = response.xpath("//div[@class='content']//p//text()").getall()

        # add the player descriptions to the output container and filter for no empty spaces
        container['description']: list = player_descriptions
        container['key'] = str(response.request.url)

        # iterate through each table using the idx integer valued
        for table_idx, keyword_name in enumerate(reference_keys.keys()):
            # iterate through each of the header names in the player keywords
            print(table_idx, keyword_name)
            for header_idx, header_name in reference_keys[keyword_name].items():
                print(header_name, header_idx)
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
            print('\n')
        yield container