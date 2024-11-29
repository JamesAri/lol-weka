import requests
import db.repository.matches as matches
import config

def GET_request(resource):
	headers = {"X-Riot-Token": config.secrets['api_key']}
	url = config.endpoints['lol_base_url'] + resource
	r = requests.get(url, headers=headers)
	return r.json()

matchesList = ['EUN1_3700431013', 'EUN1_3700362877', 'EUN1_3700344353', 'EUN1_3697216764', 'EUN1_3697189153', 'EUN1_3696737772', 'EUN1_3696706009', 'EUN1_3696663941', 'EUN1_3696222989', 'EUN1_3695694405', 'EUN1_3695669197', 'EUN1_3695292440', 'EUN1_3695267746', 'EUN1_3695238597', 'EUN1_3695230242', 'EUN1_3694652422', 'EUN1_3694245475', 'EUN1_3693455589', 'EUN1_3693448544', 'EUN1_3692328606']

def get_matches():
	resource = f"/lol/match/v5/matches/by-puuid/{config.secrets['puuid']}/ids"
	return GET_request(resource=resource)

def get_match_statistics(match_id):
    resource = f"/lol/match/v5/matches/{match_id}/timeline"
    return GET_request(resource=resource)


# print(get_matches())
# print(get_match_statistics(matchesList[0]))

if __name__ == '__main__':
	matches.saveMatches(list(map((lambda match: (match,)), matchesList)))
	allMatches = matches.getAllMatches()
	print(allMatches)