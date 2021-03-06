import dateutil.parser
import requests


ETH_BLOCKS_SUBGRAPH = (
    "https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks"
)


def get_block_for_datestring(datestring: str) -> int:

    query_timestamp = int(dateutil.parser.parse(datestring).timestamp())

    return get_block_for_timestamp(timestamp=query_timestamp)


def get_block_for_timestamp(timestamp: int):

    query = (
        f'''{{
          blocks(
          first: 1,
          orderBy: timestamp,
          orderDirection: asc,
          where: {{timestamp_gt: "{timestamp}"}}) {{
            id
            number
            timestamp
          }}
        }}
        '''
    )

    r = requests.post(ETH_BLOCKS_SUBGRAPH, json={'query': query})
    payload = dict(r.json())

    try:
        return int(payload['data']['blocks'][0]['number'])
    except KeyError:
        return None
    except IndexError:
        return None


if __name__ == "__main__":
    print(get_block_for_timestamp(timestamp=1577836800))
    print(get_block_for_datestring("20211101T2145"))
