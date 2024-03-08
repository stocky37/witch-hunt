import os
from collections import Counter
from datetime import datetime
import requests
from gql import Client, gql
import json
from gql.transport.aiohttp import AIOHTTPTransport

guild_id = 121452
twister = 9899
hatch = 9903
hours = 3
end_time = 3 * 60 * 60 * 1000


timestamp = 0
# timestamp = datetime(2024, 1, 1, 0, 0).timestamp() * 1000
wipe_cutoff = 2


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
        query(
            $guild_id: Int!,
            $start_time: Float,
            $end_time: Float,
            $timestamp: Float,
            $wipe_cutoff: Int
        ) {
            reportData {
                reports(guildID: $guild_id, zoneID: 43, startTime: $timestamp) {
                    data {
                        code
                        events(dataType: Deaths, startTime: $start_time, endTime: $end_time, wipeCutoff: $wipe_cutoff) {
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
        "end_time": end_time,
        "timestamp": timestamp,
        "wipe_cutoff": wipe_cutoff
    })
    return response["reportData"]["reports"]["data"]


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


def filter_events_by_killing_blow(report, ability):
    killing_blow = "killingAbilityGameID"
    return filter(lambda x: (killing_blow in x and x[killing_blow] == ability), report["events"]["data"])


def filter_twister_events(report):
    return filter_events_by_killing_blow(report, 9899)


def filter_hatch_events(report):
    return filter_events_by_killing_blow(report, 9903)


def main():
    twister_count = Counter()
    hatch_count = Counter()

    access_token = get_access_token()
    client = init_client(access_token)
    reports = get_reports(client)
    for report in reports:
        id_table = player_id_map(report)
        twister_events = filter_twister_events(report)
        hatch_events = filter_hatch_events(report)

        twister_count.update([id_table[x["targetID"]] for x in twister_events])
        hatch_count.update([id_table[x["targetID"]] for x in hatch_events])
    print(json.dumps(twister_count, indent=2))
    print(json.dumps(hatch_count, indent=2))


if __name__ == "__main__":
    main()
