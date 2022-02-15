import json
import re
import pytz
from dateutil import parser
from dateutil.relativedelta import relativedelta
import datetime
import os.path
import requests
from bs4 import BeautifulSoup
from decimal import Decimal, getcontext
from contextlib import redirect_stdout
import math
from numpy import nan   
import collections

BASE_URL = "https://www.espn.com"

## General functions
def retry_request(url, headers={}):
    """Get a url and return the request, try it up to 3 times if it fails initially"""
    session = requests.Session()
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=3))
    res = session.get(url=url, allow_redirects=True, headers=headers)
    session.close()
    return res

def get_soup(res):
    return BeautifulSoup(res.text, "lxml")

def get_new_json(url, headers={}):
    #print(url)
    res = retry_request(url, headers)
    if res.status_code == 200:
        return res.json()
    else:
        print("ERROR:", res.status_code)
        return {"error_code": res.status_code, "error_msg": "URL Error"}

def get_new_html_soup(url, headers={}):
    #print(url)
    res = retry_request(url, headers)
    if res.status_code == 200:
        return get_soup(res)
    else:
        print("ERROR: ESPN", res.status_code)
        return {"error_code": res.status_code, "error_msg": "ESPN Error"}

## Get constants
def get_date_leagues():
    return ["mlb","nba","ncb","ncw","wnba","nhl"]

def get_week_leagues():
    return ["nfl","ncf"]

def get_ncb_groups():
    return [50,55,56,100]

def get_ncw_groups():
    return [50,55,100]

def get_ncf_groups():
    return [80,81]

def get_leagues():
    """ Return a list of supported leagues """
    return get_week_leagues() + get_date_leagues()

def get_html_boxscore_leagues():
    return ["nhl"]

def get_no_scoreboard_json_leagues():
    """ Scoreboard json isn't easily available for some leagues, have to grab the game_ids from sportscenter_api url """
    return ["wnba", "nhl"]

def get_sport(league):
    if league in ["nba","wnba","ncb","ncw"]:
        return "basketball"
    elif league in ["mlb"]:
        return "baseball"
    elif league in ["nfl","ncf"]:
        return "football"
    elif league in ["nhl"]:
        return "hockey"

## Get urls
def get_sportscenter_api_url(sport, league, dates):
    return "https://sportscenter.api.espn.com/apis/v1/events?sport={}&league={}&dates={}".format(sport, league, dates)

def get_date_scoreboard_url(league, date, group=None):
    """ Return a scoreboard url for a league that uses dates (nonfootball)"""
    if league in get_date_leagues():
        if league == "nhl":
            return "{}/{}/scoreboard?date={}".format(BASE_URL, league, date)
        else:
            if group == None:
                return "{}/{}/scoreboard/_/date/{}?xhr=1".format(BASE_URL, league, date)
            else:
                return "{}/{}/scoreboard/_/group/{}/date/{}?xhr=1".format(BASE_URL, league, group, date)
    else:
        raise ValueError("League must be {} to get date scoreboard url".format(get_date_leagues()))

def get_week_scoreboard_url(league, season_year, season_type, week, group=None):
    """ Return a scoreboard url for a league that uses weeks (football)"""
    if league in get_week_leagues():
        if group == None:
            return "{}/{}/scoreboard/_/year/{}/seasontype/{}/week/{}?xhr=1".format(BASE_URL, league, season_year, season_type, week)
        else:
            return "{}/{}/scoreboard/_/group/{}/year/{}/seasontype/{}/week/{}?xhr=1".format(BASE_URL, league, group, season_year, season_type, week)
    else:
        raise ValueError("League must be {} to get week scoreboard url".format(get_week_leagues()))

def get_game_url(url_type, league, espn_id):
    valid_url_types = ["recap", "boxscore", "playbyplay", "conversation", "gamecast"]
    if url_type not in valid_url_types:
        raise ValueError("Unknown url_type for get_game_url. Valid url_types are {}".format(valid_url_types))
    return "{}/{}/{}?gameId={}&xhr=1".format(BASE_URL, league, url_type, espn_id)

def get_current_scoreboard_urls(league, offset=0):
    """ Return a list of the current scoreboard urls for a league 
    For date leagues optional offset is in days
    For week leagues optional offseet is in weeks """
    urls = []
    if league in get_date_leagues():
        date_str = (datetime.datetime.now() + relativedelta(days=+offset)).strftime("%Y%m%d")
        if league == "ncb":
            for group in get_ncb_groups():
                urls.append(get_date_scoreboard_url(league, date_str, group))
        elif league == "ncw":
            for group in get_ncw_groups():
                urls.append(get_date_scoreboard_url(league, date_str, group))
        else:
            urls.append(get_date_scoreboard_url(league, date_str))
        return urls
    elif league in get_week_leagues():
        # need to add timezone to now to compare with timezoned entry datetimes later
        dt = datetime.datetime.now(pytz.utc) + relativedelta(weeks=+offset)
        # guess the league season_year
        if dt.month > 2:
            guessed_season_year = dt.year
        else:
            guessed_season_year = dt.year - 1
        calendar = get_calendar(league, guessed_season_year)
        for season_type in calendar:
            if 'entries' in season_type:
                for entry in season_type['entries']:
                    if dt >= parser.parse(entry['startDate']) and dt <= parser.parse(entry['endDate']):
                        if league == "ncf":
                            for group in get_ncf_groups():
                                urls.append(get_week_scoreboard_url(league, guessed_season_year, season_type['value'], entry['value'], group))
                        else:
                            urls.append(get_week_scoreboard_url(league, guessed_season_year, season_type['value'], entry['value']))
        return urls
    else:
        raise ValueError("Unknown league for get_current_scoreboard_urls")

def get_all_scoreboard_urls(league, season_year):
    """ Return a list of all scoreboard urls for a given league and season year """
    urls = []
    if league in get_date_leagues():
        start_datetime, end_datetime = get_season_start_end_datetimes(league, season_year)
        while start_datetime < end_datetime:
            if league == "ncb":
                for group in get_ncb_groups():
                    urls.append(get_date_scoreboard_url(league, start_datetime.strftime("%Y%m%d"), group))
            elif league == "ncw":
                for group in get_ncw_groups():
                    urls.append(get_date_scoreboard_url(league, start_datetime.strftime("%Y%m%d"), group))
            else:
                urls.append(get_date_scoreboard_url(league, start_datetime.strftime("%Y%m%d")))
            start_datetime += relativedelta(days=+1)
        return urls
    elif league in get_week_leagues():
        calendar = get_calendar(league, season_year)
        for season_type in calendar:
            if 'entries' in season_type:
                for entry in season_type['entries']:
                    if league == "ncf":
                        for group in get_ncf_groups():
                            urls.append(get_week_scoreboard_url(league, season_year, season_type['value'], entry['value'], group))
                    else:
                        urls.append(get_week_scoreboard_url(league, season_year, season_type['value'], entry['value']))
        return urls
    else:
        raise ValueError("Unknown league for get_all_scoreboard_urls")

## Get stuff from URL or filenames
def get_league_from_url(url):
    return url.split('.com/')[1].split('/')[0]

def get_date_from_scoreboard_url(url):
    league = get_league_from_url(url)
    if league == "nhl":
        return url.split("?date=")[1].split("&")[0]
    else:
        return url.split('/')[-1].split('?')[0]

def get_data_type_from_url(url):
    """ Guess and return the data_type based on the url """
    data_type = None
    valid_data_types = ["scoreboard", "recap", "boxscore", "playbyplay", "conversation", "gamecast"]
    for valid_data_type in valid_data_types:
        if valid_data_type in url:
            data_type = valid_data_type
            break
    if data_type == None:
        raise ValueError("Unknown data_type for url. Url must contain one of {}".format(valid_data_types))
    return data_type

def get_filename_ext(filename):
    if filename.endswith(".json"):
        return "json"
    elif filename.endswith(".html"):
        return "html"
    else:
        raise ValueError("Uknown filename extension for {}".format(filename))

## Get requests helpers
def get_season_start_end_datetimes_helper(url):
    # TODO use cached replies if scoreboard url is older than 1 year
    scoreboard = get_url(url)
    return parser.parse(scoreboard['content']['sbData']['leagues'][0]['calendarStartDate']), parser.parse(scoreboard['content']['sbData']['leagues'][0]['calendarEndDate'])

def get_season_start_end_datetimes(league, season_year):
    """ Guess a random date in a leagues season and return its calendar start and end dates, only non football adheres to this format"""
    if league == "mlb":
        return get_season_start_end_datetimes_helper(get_date_scoreboard_url(league, str(season_year) + "0415"))
    elif league == "nba":
        return get_season_start_end_datetimes_helper(get_date_scoreboard_url(league, str(season_year - 1) + "1101"))
    elif league == "ncb" or league == "ncw":
        return get_season_start_end_datetimes_helper(get_date_scoreboard_url(league, str(season_year - 1) + "1130"))
    elif league == "wnba":
        # hardcode wnba start end dates, assumed to be April 20 thru Oct 31
        return datetime.datetime(season_year,4,20, tzinfo=pytz.timezone("US/Eastern")).astimezone(pytz.utc), datetime.datetime(season_year,10,31, tzinfo=pytz.timezone("US/Eastern")).astimezone(pytz.utc)
    elif league == "nhl":
        # hardcode nhl start end dates, assumed to be Oct 1 thru June 30
        return datetime.datetime(season_year-1,10,1, tzinfo=pytz.timezone("US/Eastern")).astimezone(pytz.utc), datetime.datetime(season_year,6,30, tzinfo=pytz.timezone("US/Eastern")).astimezone(pytz.utc)
    else:
        raise ValueError("League must be {} to get season start and end datetimes".format(get_date_leagues()))

def create_filename_ext(league, data_type):
    """ Get a filename extension (either .html or .json depending on league and data_type) """
    if league in get_html_boxscore_leagues() and data_type == "boxscore":
        return "html"
    else:
        return "json"

def get_filename(cached_json_path, league, data_type, url):
    """ Build a full filename with directories for given league, data_type and url"""
    # add slash if necessary to cached_json_path
    if cached_json_path[-1] != "/":
        cached_json_path += "/"
    dir_path = cached_json_path + "/" + league + "/" + data_type + "/"
    # create a league directory and data_type directory in cached_json if doesn't already exist
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    # create filename with / replaced with |
    filename = dir_path + url.replace('/','|')
    ext = "." + create_filename_ext(league, data_type)
    if filename.endswith(ext) == False:
        filename = filename + ext
    return filename

def get_cached(filename):
    """ Return cached json if it exists """
    data = None
    if os.path.isfile(filename):
        ext = get_filename_ext(filename)
        if ext == "json":
            with open(filename) as json_data:
                data = json.load(json_data)
        elif ext == "html":
            data = BeautifulSoup(open(filename), "lxml") 
    return data

## Get requests
def get_teams(league):
    """ Returns a list of teams with ids and names """
    teams = []
    if league == "ncf":
        # espn's college football teams page only lists fbs
        # need to grab teams from standings page instead if want all the fbs and fcs teams
        for division in ["fbs","fcs-i-aa"]:
            url = BASE_URL + "/college-football/standings/_/view/" + division
            #print(url)
            soup = get_soup(retry_request(url))
            selector = ".hide-mobile"
            team_divs = soup.select(selector)
            for team_div in team_divs:
                teams.append({'id': team_div.find("a")['href'].split('/')[-2], 'name': team_div.text})
    else:
        url = BASE_URL + "/" + league + "/teams"
        #print(url)
        soup = get_soup(retry_request(url))
        if league == "wnba":
            selector = "div.pl3"
        else:
            selector = "div.mt3"
        team_divs = soup.select(selector)
        for team_div in team_divs:
            teams.append({'id': team_div.find("a")['href'].split('/')[-2], 'name': team_div.find("h2").text})
    return teams

def get_standings(league, season_year, college_division=None):
    standings = {"conferences": {}}
    if league in ["nhl","nfl","mlb","nba","wnba","ncf","ncb","ncw"]:
        if league == "ncf" and college_division == None:
            # default to fbs
            college_division = "fbs"
        if college_division:
            valid_college_divisions = ["fbs", "fcs", "fcs-i-aa", "d2", "d3"]
            if college_division == "fcs":
                college_division = "fcs-i-aa"
            if college_division in valid_college_divisions:
                url = "{}/{}/standings/_/season/{}/view/{}".format(BASE_URL, league, season_year, college_division)
            else:
                raise ValueError("College division must be none or {}".format(",".join(valid_college_divisions)))
        elif league in ["wnba"]:
            url = "{}/{}/standings/_/season/{}/group/conference".format(BASE_URL, league, season_year)
        else:
            url = "{}/{}/standings/_/season/{}/group/division".format(BASE_URL, league, season_year)
        #print(url)
        soup = get_soup(retry_request(url))
        standings_divs = soup.find_all("div", class_="standings__table")

        for i in range(len(standings_divs)):
            conference_name = standings_divs[i].find("div", class_="Table__Title").text
            standings["conferences"][conference_name] = {"divisions": {}}
            division = "" # default blank division name
            teams_table = standings_divs[i].find("table", class_="Table--fixed-left")
            trs = teams_table.find_all("tr")
            for tr in trs:
                if "subgroup-headers" in tr["class"]:
                    division = tr.text # replace default blank division name
                    standings["conferences"][conference_name]["divisions"][division] = {"teams": []}
                elif tr.text != "":
                    if division == "" and standings["conferences"][conference_name]["divisions"] == {}:
                        standings["conferences"][conference_name]["divisions"][division] = {"teams": []}
                    team = {}
                    team_span_tag = tr.find("td", class_="Table__TD").find("span", class_="hide-mobile")
                    team_a_tag = team_span_tag.find("a")
                    if team_a_tag is None:
                        # some teams are now defunct with no espn links
                        team["name"] = team_span_tag.text.strip()
                        team["abbr"] = ""
                    else:
                        team["name"] = team_a_tag.text
                        if league in ["ncf","ncb","ncw"]:
                            team["abbr"] = team_a_tag["href"].split("/id/")[1].split("/")[0].upper()
                        else:
                            team["abbr"] = team_a_tag["href"].split("/name/")[1].split("/")[0].upper()
                    standings["conferences"][conference_name]["divisions"][division]["teams"].append(team)

    return standings
                
def get_calendar(league, date_or_season_year):
    """ Return a calendar for a league and season_year"""
    if league in get_week_leagues():
        url = get_week_scoreboard_url(league, date_or_season_year, 2, 1)
    elif league in get_date_leagues():
        url = get_date_scoreboard_url(league, date_or_season_year)
    # TODO use cached replies for older urls
    return get_url(url)['content']['calendar']

def get_url(url, cached_path=None):
    """ Retrieve an ESPN JSON data or HTML BeautifulSoup, either from cache or make new request """
    data_type = get_data_type_from_url(url)
    league = get_league_from_url(url)
    if data_type == "scoreboard":
        # for wnba and nhl we'll use a different api to retrieve game_ids and basic game data
        if league in get_no_scoreboard_json_leagues():
            url = get_sportscenter_api_url(get_sport(league), league, get_date_from_scoreboard_url(url))
    return get_cached_url(url, league, data_type, cached_path)

def get_cached_url(url, league, data_type, cached_path, headers={}):
    """ get_url helper if want to specify the league and datatype (for non espn.com links) """
    if cached_path:
        filename = get_filename(cached_path, league, data_type, url)
        data = get_cached(filename)
    else:
        data = None
    if data == None:
        ext = create_filename_ext(league, data_type)
        if ext == "json":
            data = get_new_json(url, headers)
            # dont cache if got an ESPN internal 500 error
            if cached_path and "error_code" not in data:
                with open(filename, 'w') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        elif ext == "html":
            data = get_new_html_soup(url, headers)
            if cached_path and "error_code" not in data:
                with open(filename, 'w') as f:
                    f.write(data.prettify())
    return data

# user created functions 

def fix_teams(team):
    """ This function just adds the cities to the json data to later compare with the beautifulsoup data """
    if team == "Hawks":
        team = "Atlanta Hawks: "
    if team == "Celtics":
        team = "Boston Celtics: "
    if team == "Nets":
        team = "Brooklyn Nets: "
    if team == "Hornets":
        team = "Charlotte Hornets: "
    if team == "Bulls":
        team = "Chicago Bulls: "
    if team == "Cavaliers":
        team = "Cleveland Cavaliers: "
    if team == "Mavericks":
        team = "Dallas Mavericks: "
    if team == "Nuggets":
        team = "Denver Nuggets: "
    if team == "Pistons":
        team = "Detroit Pistons: "
    if team == "Warriors":
        team = "Golden State Warriors: "
    if team == "Rockets":
        team = "Houston Rockets: "
    if team == "Pacers":
        team = "Indiana Pacers: "
    if team == "Clippers":
        team = "LA Clippers: "
    if team == "Lakers":
        team = "Los Angeles Lakers: "
    if team == "Grizzlies":
        team = "Memphis Grizzlies: "
    if team == "Heat":
        team = "Miami Heat: "
    if team == "Bucks":
        team = "Milwaukee Bucks: "
    if team == "Timberwolves":
        team = "Minnesota Timberwolves: "
    if team == "Pelicans":
        team = "New Orleans Pelicans: "
    if team == "Knicks":
        team = "New York Knicks: "
    if team == "Thunder":
        team = "Oklahoma City Thunder: "
    if team == "Magic":
        team = "Orlando Magic: "
    if team == "76ers":
        team = "Philadelphia 76ers: "
    if team == "Suns":
        team = "Phoenix Suns: "
    if team == "Trail Blazers":
        team = "Portland Trail Blazers: "
    if team == "Kings":
        team = "Sacramento Kings: "
    if team == "Spurs":
        team = "San Antonio Spurs: "
    if team == "Raptors":
        team = "Toronto Raptors: "
    if team == "Jazz":
        team = "Utah Jazz: "
    if team == "Wizards":
        team = "Washington Wizards: "
    return team

def findDiff(teamOne, teamTwo, teamAvg, teamCurr):
    """ Finds the matching team and uses its associated avg FG%
        Could be expanded to all other stats """
    if teamOne == teamTwo:
        diff = Decimal(teamAvg - teamCurr)
        return diff
    else: 
        return ""

def average_stats(soup):
    """ Grab stats for teams from the espn specific webpage - 19 columns of stats """
    q = 1
    p = 0
    #page = requests.get(url)
    #soup = BeautifulSoup(page.content, 'html.parser')
    # intialize lists that hold useful information
    teamList = []
    points = []
    fgAList = []
    fgPercList = []
    threeAList = []
    threePercList = []
    # 63 FGA # 64 FG%  #66 3PA #67 3P%
    while q < 61:
        # finding useful information from the webpage --- 19 rows of information 
        team = soup.select('td')[q].text + ": "
        pts = soup.select('td')[61+p*19].text 
        fgAttempts = (soup.select('td')[63+p*19].text)
        fgPercent = (soup.select('td')[64+p*19].text)
        threePointAttempts = (soup.select('td')[66+p*19].text)
        threePointPercent = (soup.select('td')[67+p*19].text)
        # creating lists to hold the useful information
        teamList.append(team)
        points.append(pts)
        fgAList.append(fgAttempts)
        fgPercList.append(fgPercent)
        threeAList.append(threePointAttempts)
        threePercList.append(threePointPercent)

        q += 2
        p += 1
    return teamList, points, fgAList, fgPercList, threeAList, threePercList

def cleanXLSX(column):
    i = 0
    cleaned = [i for i in column if math.isnan(i) == False]
    nanIdx = []
    while i < len(column):
        if str(column[i]) != 'nan':
            nanIdx.append(i)
        i += 1
    return nanIdx, cleaned

def match_team_logo_link(away_team, home_team):
    if away_team == "Hawks":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/atl.png&h=100&w=100'
    if away_team == "Celtics":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bos.png&h=100&w=100'
    if away_team == "Nets":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bkn.png&h=100&w=100'
    if away_team == "Hornets":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cha.png&h=100&w=100'
    if away_team == "Bulls":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/chi.png&h=100&w=100'
    if away_team == "Cavaliers":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cle.png&h=100&w=100'
    if away_team == "Mavericks":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/dal.png&h=100&w=100'
    if away_team == "Nuggets":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/den.png&h=100&w=100'
    if away_team == "Pistons":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/det.png&h=100&w=100'
    if away_team == "Warriors":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/gs.png&h=100&w=100'
    if away_team == "Rockets":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/hou.png&h=100&w=100'
    if away_team == "Pacers":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/ind.png&h=100&w=100'
    if away_team == "Clippers":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lac.png&h=100&w=100'
    if away_team == "Lakers":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lal.png&h=100&w=100'
    if away_team == "Grizzlies":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mem.png&h=100&w=100'
    if away_team == "Heat":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mia.png&h=100&w=100'
    if away_team == "Bucks":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mil.png&h=100&w=100'
    if away_team == "Timberwolves":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/min.png&h=100&w=100'
    if away_team == "Pelicans":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/no.png&h=100&w=100'
    if away_team == "Knicks":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/ny.png&h=100&w=100'
    if away_team == "Thunder":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/okc.png&h=100&w=100'
    if away_team == "Magic":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/orl.png&h=100&w=100'
    if away_team == "76ers":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phi.png&h=100&w=100'
    if away_team == "Suns":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phx.png&h=100&w=100'
    if away_team == "Trail Blazers":
       away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/por.png&h=100&w=100'
    if away_team == "Kings":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sac.png&h=100&w=100'
    if away_team == "Spurs":
       away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sa.png&h=100&w=100'
    if away_team == "Raptors":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/tor.png&h=100&w=100'
    if away_team == "Jazz":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/utah.png&h=100&w=100'
    if away_team == "Wizards":
        away_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/wsh.png&h=100&w=100'
    if home_team == "Hawks":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/atl.png&h=100&w=100'
    if home_team == "Celtics":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bos.png&h=100&w=100'
    if home_team == "Nets":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bkn.png&h=100&w=100'
    if home_team == "Hornets":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cha.png&h=100&w=100'
    if home_team == "Bulls":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/chi.png&h=100&w=100'
    if home_team == "Cavaliers":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cle.png&h=100&w=100'
    if home_team == "Mavericks":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/dal.png&h=100&w=100'
    if home_team == "Nuggets":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/den.png&h=100&w=100'
    if home_team == "Pistons":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/det.png&h=100&w=100'
    if home_team == "Warriors":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/gs.png&h=100&w=100'
    if home_team == "Rockets":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/hou.png&h=100&w=100'
    if home_team == "Pacers":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/ind.png&h=100&w=100'
    if home_team == "Clippers":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lac.png&h=100&w=100'
    if home_team == "Lakers":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lal.png&h=100&w=100'
    if home_team == "Grizzlies":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mem.png&h=100&w=100'
    if home_team == "Heat":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mia.png&h=100&w=100'
    if home_team == "Bucks":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mil.png&h=100&w=100'
    if home_team == "Timberwolves":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/min.png&h=100&w=100'
    if home_team == "Pelicans":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/no.png&h=100&w=100'
    if home_team == "Knicks":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/nyk.png&h=100&w=100'
    if home_team == "Thunder":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/okc.png&h=100&w=100'
    if home_team == "Magic":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/orl.png&h=100&w=100'
    if home_team == "76ers":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phi.png&h=100&w=100'
    if home_team == "Suns":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phx.png&h=100&w=100'
    if home_team == "Trail Blazers":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/por.png&h=100&w=100'
    if home_team == "Kings":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sac.png&h=100&w=100'
    if home_team == "Spurs":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sa.png&h=100&w=100'
    if home_team == "Raptors":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/tor.png&h=100&w=100'
    if home_team == "Jazz":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/utah.png&h=100&w=100'
    if home_team == "Wizards":
        home_logo_link = 'https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/wsh.png&h=100&w=100'
    
    return away_logo_link, home_logo_link

def fix_make_duplicates(shot_list):
    if shot_list[0] < shot_list[1]:
        last = shot_list[1] + 1
        return last
    else:
        return shot_list[0]

def grab_live_halftime_stats(game_id, soup):
    # intialize lists that hold useful information
    team_list = []
    fg_attempt_list = []
    fg_perc_list = []
    three_attempt_list = []
    three_perc_list = []
    
    # intialize looping variables
    q = 1; p = 0
    """todo - find how to trigger this function to run when halftime for each is reached"""
    #halftimeUrl = 'https://www.espn.com/nba/scoreboard'
    
    
    """ Commented out b/c we're dealing with disassociated odds """
    #games = sum(1 for line in open('cleanedOdds.txt'))
    
    #file = open('linesOdds.txt', 'wb')
    """ For grabbing the season averages for comparison """
    
    # grab season averages using average_stats
    team_list, points, fg_attempt_list, fg_perc_list, three_attempt_list, three_perc_list = average_stats(soup)
  
    url = get_game_url("boxscore", "nba", game_id)
    json_data = get_url(url)
    """ FOR GRABBING THE LIVE GAME STATS """
    # 0 index - shots made vs attempted
    # 1 index - fg%
    # 2 index - 3 point shots made vs attempted
    # 3 index - 3 point %
    # 4 index - ft's made vs attempted
    # 5 index - ft %
    # 6 index - total rebounds
    # 7 index - offensive rebounds
    # 8 index - defensive rebounds
    # 9 index - assists
    # 10 index - steals
    # 11 index - blocks
    # 12 index - turnovers
    # 13 index - team turnovers
    # 14 index - total turnovers
    # 15 index - technical fouls
    # 16 index - total technical fouls
    # 17 index - flagrant fouls
    # 18 index - points off turnovers
    # 19 index - fast break points 
    # 20 index - points in paint
    # 21 index - fouls
    # 22 index - largest lead
    # grab team names that are playing
    away_team_pre = json_data['gamepackageJSON']['boxscore']['teams'][0]['team']['name']
    home_team_pre = json_data['gamepackageJSON']['boxscore']['teams'][1]['team']['name']
    # grab fg stats associated with teams
    away_fg = json_data['gamepackageJSON']['boxscore']['teams'][0]['statistics'][1]
    home_fg = json_data['gamepackageJSON']['boxscore']['teams'][1]['statistics'][1]
    # grab shot and attempt data 
    away_shots = json_data['gamepackageJSON']['boxscore']['teams'][0]['statistics'][0]
    home_shots = json_data['gamepackageJSON']['boxscore']['teams'][1]['statistics'][0]
    # convert for ease of use
    away_fg = json.dumps(away_fg)
    home_fg = json.dumps(home_fg)
    away_shots = json.dumps(away_shots)
    home_shots = json.dumps(home_shots)

    # grab the image link associated with each team 
    away_team_logo_link, home_team_logo_link = match_team_logo_link(away_team_pre, home_team_pre)
    """ Continuing on grabbing the CURRENT halftime stats for direct comparison with season averages """
    # fix names to help with comparing
    away_team = fix_teams(away_team_pre)
    home_team = fix_teams(home_team_pre)

    # find the shooting percentages by recreating the number digit by digit
    shooting = re.search(r"\d", away_fg)
    first_digit = shooting.start(); second_digit = first_digit + 1; third_digit = second_digit + 1; fourth_digit = third_digit + 1
    # current FG percentages at halftime
    away_percentage = Decimal(away_fg[first_digit] + away_fg[first_digit+1] + away_fg[first_digit+2] + away_fg[first_digit+3])
    home_percentage = Decimal(home_fg[first_digit] + home_fg[first_digit+1] + home_fg[first_digit+2] + home_fg[first_digit+3])
    # grab the shot makes and total attempts currently at half 
    away_shots_made = Decimal(away_shots[first_digit] + away_shots[first_digit+1]); away_shots_total = Decimal(away_shots[first_digit+3] + away_shots[first_digit+4])
    home_shots_made = Decimal(home_shots[first_digit] + home_shots[first_digit+1]); home_shots_total = Decimal(home_shots[first_digit+2] + home_shots[first_digit+3])

    """ Do some calculations """
    # index that the team appears in the SEASON AVERAGE table sorted by FG%
    away_match_idx = team_list.index(away_team)
    home_match_idx = team_list.index(home_team)
    # find the avg FG%
    away_match_avg = Decimal(fg_perc_list[away_match_idx])
    home_match_avg = Decimal(fg_perc_list[home_match_idx]) 
    # calculate the FG% difference
    away_diff = away_match_avg - away_percentage
    home_diff = home_match_avg - home_percentage

    playbyplay = requests.get('https://www.espn.com/nba/playbyplay/_/gameId/' + game_id) 
    soup = BeautifulSoup(playbyplay.content, 'html.parser')

    details = [deets.text for deets in soup.find_all("td", {"class" : "game-details"})]
    half_score = [deets.text for deets in soup.find_all("td", {"class" : "combined-score"})]
    for j in range(len(details)):
        if details[j] == 'End of the 2nd Quarter':
            half_score = half_score[j]
    shooting = re.search(r"\d", half_score)
    first_digit = shooting.start(); second_digit = first_digit + 1; third_digit = second_digit + 1
    away_points = (half_score[first_digit] + half_score[second_digit])
    home_points = (half_score[third_digit+3] + half_score[third_digit+4])
    return away_team_pre, away_team_logo_link, away_points, away_shots_made, away_shots_total, away_fg, home_team_pre, home_team_logo_link, home_points, home_shots_made, home_shots_total, home_fg  

def grab_first_half_stats(game_id, away_team_link, home_team_link, away_name, home_name):

    
    """First Game of the Season --- 401359833 --- Pacers @ Hornets"""
    """Last Game of the Season --- 401361049 --- Jazz @ Trail Blazers"""
    playbyplay = requests.get('https://www.espn.com/nba/playbyplay/_/gameId/' + game_id) 
    soup = BeautifulSoup(playbyplay.content, 'html.parser')
    deets = []

    logoList = []
    for logo in soup.find_all("img", {"class" : "team-logo"}): 
        logoList.append(logo.get('src'))

    details = [deets.text for deets in soup.find_all("td", {"class" : "game-details"})]
    half_score = [deets.text for deets in soup.find_all("td", {"class" : "combined-score"})]
    half = []
    for j in range(len(details)):
        if details[j] == 'End of the 2nd Quarter':
            half.append(j)
            half_score = half_score[j]
    shooting = re.search(r"\d", half_score)
    first_digit = shooting.start(); second_digit = first_digit + 1; third_digit = second_digit + 1
    away_points = Decimal(half_score[first_digit] + half_score[second_digit])
    home_points = half_score[third_digit+3] + half_score[third_digit+4]
    idxHome = []
    idxAway = []
    i = 0
    
    for i in range(len(logoList)):
        if logoList[i] == home_team_link:
            idxHome.append(i)
            if i >= half[0] + 1:
                break
        if logoList[i] == away_team_link:
            idxAway.append(i)
            if i >= half[0] + 1:
                break
    # fixing home and away lists --- its always two extra logos for the home and 1 extra logo for the away
    idxHome = idxHome[2:]
    idxHome = [x - 3 for x in idxHome]
    idxAway = idxAway[1:] 
    idxAway = [x - 3 for x in idxAway]
    k = 0
    awayEvents = []
    homeEvents = []
    while k < len(idxAway):
        awayEvents.append(details[idxAway[k]])
        k += 1
    k = 0
    while k < len(idxHome):
        homeEvents.append(details[idxHome[k]])
        k += 1

    if (len(idxHome) - len(idxAway)) > 0:
        moreEvents = len(idxHome)
    else:
        moreEvents = len(idxAway)

    """ Using the shot chart from the espn website """
    # finding home team shooting stats 
    homeTeam = soup.find('ul', {'class' : 'shots home-team'})
    homeMake = [deets.text for deets in homeTeam.findChildren("li", {"class" : "made"}, recursive=False)]
    homeMiss = [deets.text for deets in homeTeam.findChildren("li", {"class" : "missed"}, recursive=False)]
    homeTotalShots = len(homeMake) + len(homeMiss)

    # finding away team shooting stats
    awayTeam = soup.find('ul', {'class' : 'shots away-team'})
    awayMake = [deets.text for deets in awayTeam.findChildren("li", {"class" : "made"}, recursive=False)]
    awayMiss = [deets.text for deets in awayTeam.findChildren("li", {"class" : "missed"}, recursive=False)]
    awayTotalShots = len(awayMake) + len(awayMiss) 

    q = 0
    p = 0
    # grab home make and miss data relative to the events
    homeMissList = []
    homeMakeList = []
    checkHomeMiss = 0
    checkHomeMake = 0
    homeEvents.reverse()
    while p <= moreEvents:
        while q <= len(homeMiss):
            if p == len(homeEvents):
                break
            if homeEvents[p] != homeMiss[q]:
                q += 1
                if q == len(homeMiss):
                    p += 1
                    q = 0
            elif homeEvents[p] == homeMiss[q]:
                checkHomeMiss = homeMiss.index(homeEvents[p])
                homeMissList.append(checkHomeMiss)
                p += 1
        break
    p = 0; q = 0
    while p <= moreEvents:
        while q <= len(homeMake):
            if p == len(homeEvents):
                break
            if homeEvents[p] != homeMake[q]:
                q += 1
                if q == len(homeMake):
                    p += 1
                    q = 0
            elif homeEvents[p] == homeMake[q]:
                checkHomeMake = homeMake.index(homeEvents[p])
                homeMakeList.append(checkHomeMake)
                p += 1
        break

    # grab away make and miss data relative to the events 
    awayMissList = []
    awayMakeList = []
    checkAwayMiss = 0
    checkAwayMake = 0
    awayEvents.reverse()
    p = 0; q = 0
    while p <= moreEvents:
        while q <= len(awayMiss):
            if p == len(awayEvents):
                break
            if awayEvents[p] != awayMiss[q]:
                q += 1
                if q == len(awayMiss):
                    p += 1
                    q = 0
            elif awayEvents[p] == awayMiss[q]:
                checkAwayMiss = awayMiss.index(awayEvents[p])
                awayMissList.append(checkAwayMiss)
                p += 1
        break
    p = 0; q = 0
    while p <= moreEvents:
        while q <= len(awayMake):
            if p == len(awayEvents):
                break
            if awayEvents[p] != awayMake[q]:
                q += 1
                if q == len(awayMake):
                    p += 1
                    q = 0
            elif awayEvents[p] == awayMake[q]:
                checkAwayMake = awayMake.index(awayEvents[p])
                awayMakeList.append(checkAwayMake)
                p += 1
        break

    
    # find matching index 
    lastHomeMake = homeMake.index(homeMake[homeMakeList[0]])
    lastHomeMiss = homeMiss.index(homeMiss[homeMissList[0]])
    lastAwayMake = awayMake.index(awayMake[awayMakeList[0]])
    lastAwayMiss = awayMiss.index(awayMiss[awayMissList[0]])
    # fix if there are duplicates
    lastHomeMake = fix_make_duplicates(homeMakeList)
    lastHomeMiss = fix_make_duplicates(homeMissList)
    lastAwayMake = fix_make_duplicates(awayMakeList)
    lastAwayMiss = fix_make_duplicates(awayMissList)
    
    # cut lists to approptiate indexes - all first half data
    awayMake = awayMake[:(lastAwayMake+1)]; awayMakeLength = len(awayMake)
    awayMiss = awayMiss[:(lastAwayMiss+1)]; awayMissLength = len(awayMiss); awayTotalAttempts = awayMissLength + awayMakeLength
    homeMake = homeMake[:(lastHomeMake+1)]; homeMakeLength = len(homeMake)
    homeMiss = homeMiss[:(lastHomeMiss+1)]; homeMissLength = len(homeMiss); homeTotalAttempts = homeMissLength + homeMakeLength



    # calculate fg% 
    homeFG = round(homeMakeLength / (homeMakeLength + homeMissLength),4)*100
    awayFG = round(awayMakeLength / (awayMakeLength + awayMissLength),4)*100

    return awayMakeLength, awayMissLength, awayFG, homeMakeLength, homeMissLength, homeFG    

def val_append(dictionary, key1, key2, value):
    value = [value]
   
    if key1 not in dictionary:
       
        dictionary.setdefault(key1,{})[key2] = value
        
   
    elif key1 in dictionary and key2 in dictionary[key1]:
    
        dictionary[key1][key2].append(value)

    else:
        dictionary.setdefault(key1,{})[key2] = value

    return dictionary


