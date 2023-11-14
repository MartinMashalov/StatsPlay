# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd
from .spiders.models import find_most_similar_key
from typing import List, Any, Union
from pprint import pprint
import usaddress
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import warnings
import toml
import os
warnings.filterwarnings("ignore")


class DBSettings(BaseModel):
    """configure the db settings in the base model"""
    username: str = 'doadmin'
    password: str = '7X52yc0N8Z94bvY1'
    host: str = f"mongodb+srv://{username}:{password}@db-mongodb-nyc1-60998-0cb266db.mongo.ondigitalocean.com"


class ParserSettings(BaseModel):
    """parsing settings and configurations"""

    # define the years that the parser should ignore type switching on
    years: list = [str(i) for i in range(2000, 2030)]


class GeneralParserPipeline(object):

    def __init__(self):
        # create the command parsing mapping for use in data cleaning
        self.command_pattern_parsing: dict = {
            True: [self.dataframe_cleaning_vector, self.dataframe_postprocessing,
                       self.dataframe_height_fet_conversion, self.dataframe_capitalize,
                   self.dataframe_school_postprocess,
                   self.dataframe_nans_postprocess, self.dataframe_origin_format,self.dataframe_position_postprocess],
            False: [self.dataframe_cleaning_vector, self.dataframe_postprocessing, self.dataframe_capitalize,
                    self.dataframe_nans_postprocess]
        }
        # initiate the parser settings base model
        self.parser_settings = ParserSettings()
        current_script_directory = os.path.dirname(os.path.abspath(__file__))
        config_file_path = os.path.join(current_script_directory,"spiders", "pipeline_configs.toml")
        # read the configuration file
        self.config = toml.load(config_file_path)

    @staticmethod
    def _cleaning_vector(tag_to_clean: str) -> str:
        """clean the data in the pandas data frame"""

        # check if the tag is valid for cleaning operation
        if '<td' in str(tag_to_clean) or '/td' in str(tag_to_clean):
            return str(tag_to_clean).split('</td')[0].split('</a>')[0].split('>')[-1]
        else:
            return str(tag_to_clean)

    @staticmethod
    def format_origin(location: str) -> str:
        """format the origin of the player"""
        parsed_addr = usaddress.parse(location)
        formatted_parts = []

        for part, label in parsed_addr:
            if label == 'PlaceName':
                formatted_parts.append(part.title())
            elif label == 'StateName':
                formatted_parts.append(part.upper())
            else:
                formatted_parts.append(part)

        return ' '.join(formatted_parts)

    def _postprocessing_types_ext(self, item: str) -> str:
        """postprocess the strings as extension to previous stages - remove extra strings and convert types"""

        # check if item is a year
        if item in self.parser_settings.years:
            return str(item)

        # remove any byte-strings
        item = item.replace("\xa0", "").strip().capitalize()

        # attempt to change the types of floats currently represented in strings
        try:
            item = float(item)
        except ValueError:
            pass

        # capitalize the output item
        item: Any = item.capitalize() if isinstance(item, str) else item

        return item

    def _postprocessing_origins(self, item: str) -> str:
        """postprocessing for the origin of the players"""

        # check if only the state is present
        item = item.lower().replace(' usa', '').title()
        parsed_item = item.split(',')
        if parsed_item[0] == '':
            return parsed_item[-1].strip().upper()

        # if the second placeholder is a USA state
        if len(parsed_item[-1]) == 2:
            item = item.replace(parsed_item[-1], parsed_item[-1].upper())

        # format the normal location
        return self.format_origin(item)

    def dataframe_cleaning_vector(self, df: pd.DataFrame) -> pd.DataFrame:
        """cleaning vector command method"""
        return df.applymap(self._cleaning_vector)

    def dataframe_postprocessing(self, df: pd.DataFrame) -> pd.DataFrame:
        """postprocessing of dataframe across all columns"""
        return df.applymap(self._postprocessing_types_ext)

    def dataframe_height_fet_conversion(self, df: pd.DataFrame) -> pd.DataFrame:
        """processing of dataframe height feature with proper parsing"""

        # check if dataframe contains the height feature
        if 'height' in list(df.columns):
            # convert the heights to proper conventions
            df['height'] = df['height'].apply(lambda x: x.replace("\'", ',').replace('"', ''))

        return df

    def dataframe_team_league_parsing(self, df: pd.DataFrame) -> pd.DataFrame:
        """parsing of team"""

        # instantiate the league variable
        league: Union[str, None] = None

        # edit the league
        if 'league' in df.columns:
            # make all the league abbreviations uppercase
            df['league'] = df['league'].apply(lambda x: x.upper())
            try:
                league: str = df['league'].iloc[0].lower()
            except (KeyError, IndexError):
                league = None

        # check if team is in the column headers as a feature
        if 'team' in df.columns:
            # make everything lowercase
            df['team'] = df['team'].apply(lambda x: x.lower())

            # remove the league from the team
            df['team'] = df['team'].apply(lambda x: x.replace(f'{league}, ', '').title())

        return df

    def _search_school(self, school: str) -> str:
        """search for the correct school autocorrect in the configuration file"""

        # error handling
        try:
            return self.config['parsing']['schools'][school]
        except KeyError:
            return school.title()

    def dataframe_school_postprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """process the schools to match proper naming convention"""

        # check if dataframe has the college feature
        if 'college' in list(df.columns):
            df['college'] = df['college'].apply(lambda x: x.lower()) # make everything lowercase for future comparison

            # replace the college names with the official names and handle missing colleges from list
            df['college'] = df['college'].apply(lambda x: self._search_school(x))
            df['college'] = df['college'].apply(lambda x: x.replace('st.', 'State').replace('St.', 'State'))

            # check if it is a three letter college - an abbreviation for a school
            df['college'] = df['college'].apply(lambda x: x.upper() if len(x) == 3 else x)

        return df

    def dataframe_nans_postprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """dataframe nans postprocessing"""

        # remove all the None strings and nans from the dataframe
        df = df.applymap(lambda x: x.replace('None', '') if x == 'None' else x)
        return df

    def dataframe_position_postprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """postprocess the position that a player is playing in"""

        # check if the position is in the dataframe features
        if 'position' in list(df.columns):
            # change all the positions to uppercase
            df['position'] = df['position'].apply(lambda x: x.upper() if isinstance(x, str) else x)

        return df

    def dataframe_capitalize(self, df:pd.DataFrame) -> pd.DataFrame:
        """capitalize all the items in the dataframe"""
        return df.applymap(lambda x: x.title() if isinstance(x, str) else x)

    def dataframe_origin_format(self, df:pd.DataFrame) -> pd.DataFrame:
        """format all the origins of players in the pandas dataframe"""

        # check if origins is a feature of the dataframe
        if 'origin' in list(df.columns):
            # remove country code from the location if it is in the USA
            df['origin'] = df['origin'].apply(lambda x: x.replace(' Usa', '').replace('usa', '').replace('Usa', ''))

            # edit the origin based on the address conventions in the USA
            df['origin'] = df['origin'].apply(lambda x: self._postprocessing_origins(x))

        return df

    def _dataframe_querying(self, value: Any, mode: bool) -> dict:
        """conduct all the dataframe querying required to clean data using external function"""

        # read item into pandas dataframe from dictionary object
        df = pd.DataFrame.from_dict(value, orient='index').transpose()

        # implement the command method across all dataframe commands
        for command in self.command_pattern_parsing[mode]:
            df = command(df)

        # fix the orientation of the dataframe
        return df.to_dict(orient='list')

    def _compute_parsing_mode(self, sport: str, key: str) -> bool:
        """compute the parsing mode needed for the querying algorithm"""

        if sport == 'nfl':
            return key == 'roster'
        elif sport == 'mlb':
            return True
        elif sport == 'nba':
            return True
        else:
            return True

    def process_item(self, item, spider) -> dict:
        """parse the tags of the html using pandas vector processing"""

        # load in the item to the item adapter facade class
        item: dict = dict(item)

        # attempt to calculate the sport
        try:
            sport: Union[str, None] = item['sport']
        except KeyError:
            sport: Union[str, None] = None

        # iterate through each of the fields
        for key, value in item.items():
            # skip fields that are not for editing
            if key == 'description' or key == 'key' or key == 'sport':
                continue
            # clean out the updated date
            elif key == 'updated':
                item[key] = value.split(':')[-1].strip().capitalize() if isinstance(value, str) else value
                continue

            # add the queried dataframe to the item output container
            item[key] = self._dataframe_querying(value, mode=self._compute_parsing_mode(sport, key))

        return item


class PlayerDescriptionSetting(BaseModel):
    """settings model for player description parsing functionality"""

    # define the header index for the player parsing keys
    header_idx: dict = {
        'nba': ['name', 'born', 'origin', 'position', 'height', 'weight', 'draft', 'college', 'career'],
        'mlb': ['name', 'born', 'origin', 'position', 'bat', 'throw', 'height',
                'weight', 'draft', 'college', 'career'],
        'nfl': ['name', 'born', 'origin', 'position', 'height', 'weight', 'draft', 'college', 'career'],
        'nhl': ['name', 'born', 'origin', 'position', 'height', 'weight', 'shoot/catch', 'career']
    }

    # create index of possible synonyms
    header_synonyms: dict = {'name': ['full_name'], 'born': ['born'], 'position':
        ['position', 'primary_position'], 'height': ['height'], 'weight': ['weight'], 'draft': ['draft'],
            'college': ['college', 'school'], 'career': ['career'], 'bat': ['bats'], 'throw': ['throws'],
                             'shoot/catch': ['shoot', 'catch', 'shoot/catch']}


class PlayerDescriptionPipeline(GeneralParserPipeline, object):

    # instantiate the base model with all settings pertaining to player description processing
    player_desc_model = PlayerDescriptionSetting()

    def __init__(self):
        super().__init__()
        self.description_command_pattern: list = [
            self.dataframe_description_postprocessing, self.dataframe_capitalize, self.dataframe_origin_format,
            self.dataframe_postprocessing,
            self.dataframe_height_fet_conversion, self.dataframe_school_postprocess,self.dataframe_position_postprocess]

    def _select_subframe_lang(self, df: pd.DataFrame, header: str, headers: list, col_ref: str = 'desc'):
        """select the subframe of the dataframe based on the language context"""

        # get the reference key synonyms associated with the header
        sym_keys: list = self.player_desc_model.header_synonyms[header]

        # create a stopping counter
        stop_counter: int = 0

        # create the starting sub dataframe
        sub_df: Union[list, pd.DataFrame] = []

        # loop through until information is found otherwise continue
        while len(sub_df) == 0 and stop_counter <= len(sym_keys)-1:
            # filter the rows of the dataframe
            sub_df = df[df[col_ref].str.contains(f'{sym_keys[stop_counter]}:'.lower().replace('_', ' '))]
            stop_counter += 1

        # clean the dataframe
        sub_df['desc'] = sub_df['desc'].apply(lambda x: x.replace('\n', '').lower())

        # rename the column header of the dataframe based on similarity of keys
        try:
            sub_df.rename(columns={'desc': find_most_similar_key(sub_df['desc'].iloc[0].split(':')[0], headers)},
                      inplace=True)
        except IndexError:
            pass

        return sub_df

    @staticmethod
    def _preprocessing_description(description: list) -> list:
        """preprocessing the description for basic issues"""

        # cleaning operation
        description: List[str] = [item for item in description if isinstance(item, str) and 'to sort' not in item]
        return description

    def _postprocessing_description(self, sub_df: pd.DataFrame, header: str, container: dict) -> dict:
        """postprocessing of the description"""

        # get the column name
        col_name = list(sub_df.columns)[0]

        # convert the dataframe to a list type
        data = sub_df[col_name].to_list()

        # do error handling to deal with different fields of player descriptions
        try:
            header_data = data[0]
            try:
                header_data = float(header_data) if header_data not in self.parser_settings.years else header_data
            except ValueError:
                pass
        except IndexError:
            header_data = ''

        # postprocess the header data
        header_data = header_data.strip() if isinstance(header_data, str) else header_data
        if header == 'position':
            header_data = header_data.replace('-', ', ') if isinstance(header_data, str) else header_data

        # add the header data to the container
        #if ':' in header_data:
        #    header_data = ''
        container[header] = header_data
        return container

    def dataframe_description_postprocessing(self, df: pd.DataFrame) -> pd.DataFrame:
        """postprocess the dataframe description"""

        # edit the column to remove unnecessary strings
        df = df.applymap(lambda x: x.split(': ')[-1].replace("\'", "'"))

        # check if shooting hand is in the dataframe
        if 'shoot/catch' in df.columns:
            # find the hand and capitalize the first letter
            df['shoot/catch'] = df['shoot/catch'].apply(lambda x: x[0].upper())

        return df

    def _dataframe_querying_desc(self, df: pd.DataFrame) -> pd.DataFrame:
        """query the database and execute all the command in the pattern mapping"""

        # iterate through all the commands to overwrite the dataframe
        for command in self.description_command_pattern:
            # execute the command
            df = command(df)
        return df

    def _process_description_origin(self, df: pd.DataFrame) -> str:
        """process the origin of the player description"""

        # define the value in the dataframe
        value: Any = df['desc'].iloc[2]

        # process the origins according to the convention adn then filter for any unwanted values
        return self._postprocessing_origins(value).strip().title() if ':' not in value else ''

    def _process_description_name(self, df: pd.DataFrame) -> str:
        """process the description name with filtering process as well"""

        # get the value associated with the player name
        value: str = df['desc'].iloc[0]

        # filter and return
        return value.title().strip() if ':' not in value else ''

    @staticmethod
    def _prepare_player_description(data: list) -> list:
        """prepare the player description for the postprocessing stage"""

        # create copy of the data
        data_copy: list = data.copy()

        # find the index of the draft component
        try:
            drafted_idx: int = data_copy.index('Drafted:')
        except ValueError:
            return data_copy

        # create the new drafted component with string merging
        new_draft_comp: str = f'{data_copy[drafted_idx]} {data_copy[drafted_idx + 1]}'

        # add the new component and then remove the old faulty version
        data_copy[drafted_idx] = new_draft_comp
        del data_copy[drafted_idx + 1]

        # remove unnecessary keywords from the player description
        data_copy = [item for item in data_copy if item != '\n']
        data_copy = [item for item in data_copy if 'click' not in item.lower()]
        data_copy = [item for item in data_copy if 'associated' not in item.lower()]
        data_copy = [item for item in data_copy if 'sporting news' not in item.lower()]
        data_copy = [item for item in data_copy if 'writers' not in item.lower()]
        return data_copy

    def _parse_description(self, description: list, sport: str = 'mlb') -> dict:
        """parse the player description"""

        # set the keyword headers and container
        headers, container = self.player_desc_model.header_idx[sport], {}

        # clean out the description before use
        description = self._prepare_player_description(description)

        # put data description into a pandas dataframe
        df = pd.DataFrame(data={'desc': description}, columns=['desc'])
        df['desc'] = df['desc'].apply(str.lower)

        # iterate through each header
        for header in headers:
            # check if we are parsing for the full name of the player
            if header == 'name':
                container[header] = self._process_description_name(df) # process the name of the player to be titled
            elif header == 'origin':
                container[header] = self._process_description_origin(df)
            else:
                # query for a sub dataframe
                sub_df = self._select_subframe_lang(df, header, headers)

                # conduct the dataframe querying with the command design pattern
                sub_df = self._dataframe_querying_desc(sub_df)

                # postprocessing stage
                container = self._postprocessing_description(sub_df, header, container)

        return container

    def process_item(self, item, spider) -> dict:
        """process the player description here"""

        # load in the item in item adapter facade
        item: dict = dict(item)

        if 'description' in item.keys():
            # get the corresponding data and do preliminary data cleaning on the description
            data, sport = self._preprocessing_description(item['description']), item['sport']

            # fetch the parsing results
            updated_container: dict = self._parse_description(data, sport=sport)

            # add all the keys and value pairs to the existing item
            item['player_description'] = updated_container

            # delete the description general field
            del item['description']

        return item


# create the mongodb collection and connection
settings_model = DBSettings()
try:
    client = MongoClient(settings_model.host)
    db = client['Match']
except ConnectionFailure:
    raise ("Connection Failed")


class InsertItemPipeline(object):

    def __init__(self):
        self.settings = DBSettings()

    def _create_db_collection(self, item: dict, sport_key: str):
        """create the database collection"""

        # check the keys of the item
        if 'standings' in item['key']:
            return db[f'{sport_key}_standings']
        elif 'roster' in item['key']:
            return db[f'{sport_key}_roster']
        elif 'leaders' in item['key']:
            return db[f'{sport_key}_leaders']
        elif 't-' in item['key']:
            return db[f'{sport_key}_teams']
        elif 'p-' in item['key']:
            return db[f'{sport_key}_players']
        else:
            return db[f'{sport_key}_Other']

    def process_item(self, item, spider) -> str:
        """insert the item in the database"""
        pprint(item)
        # create the database collection
        collection = self._create_db_collection(item, sport_key=item['sport'])
        # check if item exists and then add it to the collection
        try:
            if len(list(collection.find({'key': item['key']}))) == 0:
                collection.insert_one(item)
            else:
                collection.update_one({'key': item['key']}, {'$set': item}, upsert=True)
            return "200"
        except:
            return "404"