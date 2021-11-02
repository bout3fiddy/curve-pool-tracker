import datetime
import logging
import os
import click

import brownie
import pandas

from utils.curve_utils.registry import get_eth_pools
from utils.eth_blocks_utils import get_block_for_datestring
from utils.network_utils import connect_if_not_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d-4s %(levelname)-4s [%(filename)s "
    "%(module)s:%(lineno)d] :: %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S",
)
DATA_OUTPUT_DIRECTORY = "./data"


@click.command()
@click.option("--date-start", "date_start", type=str)
@click.option("--date-end", "date_end", type=str)
@click.option("--filename", "filename", type=str)
def main(date_start: str, date_end: str, filename: str):

    block_start = get_block_for_datestring(date_start)
    block_end = get_block_for_datestring(date_end)
    blocks = list(range(block_start, block_end))

    logging.info(
        f"Querying between: "
        f"{block_start} ({date_start}) : {block_end} ({date_end})"
    )

    connect_if_not_connect()
    eth_pools = get_eth_pools()

    swap_revenue_data = pandas.DataFrame()
    filename = os.path.join(DATA_OUTPUT_DIRECTORY, filename)
    if os.path.exists(filename):
        logging.info(f"File {filename} exists. Loading ...")
        swap_revenue_data = pandas.read_parquet(filename)

    try:

        for block_number in blocks:

            timestamp = brownie.web3.eth.getBlock(block_number)['timestamp']
            logging.info(
                f"Processing data for block: {block_number} ({timestamp})"
            )
            calculation_time_start = datetime.datetime.now()
            total_calculation_time = 0

            pool_data = {
                "timestamp": [],
                "block_number": [],
                "pool_name": [],
                "pool_addr": [],
                "lp_token_addr": [],
                "lp_token_virtual_price": [],
                "total_supply_lp_token": []
            }

            with brownie.multicall(block_identifier=block_number):

                for pool_name, pool_details in eth_pools.items():

                    if not swap_revenue_data.empty and pool_data_exists(
                            pool_name,
                            block_number,
                            swap_revenue_data
                    ):
                        logging.debug(
                            f"Block {block_number} for pool {pool_name} "
                            f"already computed ... skipping."
                        )
                        continue

                    pool = pool_details[0]
                    lp_token = pool_details[1]

                    virtual_price = pool.get_virtual_price() * 1e-18
                    total_supply_lp_token = lp_token.totalSupply() * 1e-18

                    # pool data
                    pool_data['timestamp'].append(timestamp)
                    pool_data['block_number'].append(block_number)
                    pool_data['pool_name'].append(pool_name)
                    pool_data['pool_addr'].append(pool.address)
                    pool_data['lp_token_addr'].append(lp_token.address)
                    pool_data['lp_token_virtual_price'].append(virtual_price)
                    pool_data['total_supply_lp_token'].append(
                        total_supply_lp_token
                    )

            calculation_time = (
                    datetime.datetime.now() - calculation_time_start
            ).total_seconds()
            logging.info(
                f"Calculation for 1 block took "
                f"{round(calculation_time, 2)} seconds. "
            )
            logging.info(
                f"Elapsed querying time: "
                f"{round(total_calculation_time/60, 2)} minutes. "
                f"Estimated time left: "
                f"{round(calculation_time * len(blocks) / 60, 2)} minutes."
            )

            pool_data = pandas.DataFrame(
                data=pool_data,
                index=[pandas.to_datetime(pool_data['timestamp'], unit='s')]
            )

            # join df
            swap_revenue_data = pandas.concat([swap_revenue_data, pool_data])

        logging.info(f"Saving queried data into {filename} ...")
        swap_revenue_data.to_parquet(filename)

    except Exception as err:  # catch all exceptions

        if not swap_revenue_data.empty:
            swap_revenue_data.to_parquet(filename)

        raise err


def pool_data_exists(
        pool_name: str,
        block_number: int,
        data: pandas.DataFrame
):
    return (
            (data['block_number'] == block_number) &
            (data['pool_name'] == pool_name)
    ).any()


if __name__ == "__main__":
    main()
