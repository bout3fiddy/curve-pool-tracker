import os

import click
import pandas

from query_pool_data import DATA_DIRECTORY


@click.command()
@click.option("--input-filename", "input_filename", type=str)
@click.option("--output-filename", "output_filename", type=str)
def main(input_filename: str, output_filename: str):

    input_filename = os.path.join(DATA_DIRECTORY, input_filename)
    output_filename = os.path.join(DATA_DIRECTORY, output_filename)

    if not os.path.exists(input_filename):
        pass

    queried_data = pandas.read_parquet(input_filename)
    delta_swap_revenue_data = pandas.DataFrame()
    for pool_name in queried_data.pool_name.unique():

        pool_data = queried_data[queried_data.pool_name == pool_name]
        pool_data = pool_data.sort_index(ascending=True)

        pool_data['virtual_price_diff'] = (
            pool_data.lp_token_virtual_price.diff()
        )
        pool_data['swap_fee_revenue'] = (
                pool_data.virtual_price_diff *
                pool_data.total_supply_lp_token * 2
        )

        delta_swap_revenue_data = pandas.concat(
            [delta_swap_revenue_data, pool_data]
        )

    delta_swap_revenue_data.to_parquet(output_filename)


if __name__ == "__main__":
    main()
