import os
from collections import Counter

import requests
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

guild_id = 121452
twister = 9899
hours = 3
end_time = 3 * 60 * 60 * 1000


def get_access_token():
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]

    oauth_response = requests.post(
        "https://www.fflogs.com/oauth/token",
        auth=(client_id, client_secret),
        files={"grant_type": (None, "client_credentials")}
    )

    return oauth_response.json()["access_token"]


def init_client(token):
    transport = AIOHTTPTransport(
        url="https://www.fflogs.com/api/v2/client",
        headers={"Authorization": "Bearer {0}".format(token)}
    )
    return Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=30)


def get_reports(session):
    q = gql(
        """
        query($guild_id: Int!, $start_time: Float, $end_time: Float) {
            reportData {
                reports(guildID: $guild_id, zoneID: 43) {
                    data {
                        code
                        events(dataType: Deaths, startTime: $start_time, endTime: $end_time) {
                            data
                        }
                        playerDetails(startTime: $start_time, endTime: $end_time)
                    }
                }
            }
        }
        """
    )

    response = session.execute(q, variable_values={
        "guild_id": guild_id,
        "start_time": 0,
        "end_time": end_time
    })
    return response["reportData"]["reports"]["data"]


def get_events(session, report):
    q = gql(
        """
        query($code: String!, $start_time: Float, $end_time: Float) {
            reportData {
                report(code: $code) {
                    code
                    events(dataType: Deaths, startTime: $start_time, endTime: $end_time) {
                        data
                    }
                    playerDetails(startTime: $start_time, endTime: $end_time)
                }
            }
        }
        """
    )

    response = session.execute(q, variable_values={
        "code": report["code"],
        "start_time": 0,
        "end_time": report["endTime"] - report["startTime"]
    })
    return response


def player_id_map(report):
    id_map = {}
    player_details = report["playerDetails"]["data"]["playerDetails"]
    for player in player_details["healers"]:
        id_map[player["id"]] = player["name"]
    for player in player_details["tanks"]:
        id_map[player["id"]] = player["name"]
    for player in player_details["dps"]:
        id_map[player["id"]] = player["name"]
    return id_map


def twister_filter(event):
    ability = "killingAbilityGameID"
    return ability in event and event[ability] == twister


def filter_twister_events(report):
    return list(filter(twister_filter, report["events"]["data"]))


def main():
    twister_count = Counter()

    access_token = get_access_token()
    client = init_client(access_token)
    reports = get_reports(client)
    for report in reports:
        id_table = player_id_map(report)
        twister_events = filter_twister_events(report)
        twister_count.update([id_table[x["targetID"]] for x in twister_events])
    print(twister_count)


if __name__ == "__main__":
    main()

event_query = gql(
    """
    query($report_code: String!, $fight_ids: [Int]) {
        reportData {
            report(code: $report_code) {
                code
                events(dataType: Deaths, fightIDs: $fight_ids) {
                    data
                }
                playerDetails(fightIDs: $fight_ids)
            }
        }
    }
    """
)