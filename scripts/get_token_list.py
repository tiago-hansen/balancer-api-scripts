import requests
import pandas as pd
import pygsheets
import logging

import os


def main(chain, spreadsheet_name, worksheet_name):
    # API parameters
    API_URL = "https://api-v3.balancer.fi"
    QUERY = f"""
    {{
      tokenGetTokens(chains: [{chain}]) {{
        chain
        symbol
        underlyingTokenAddress
        address
        priceRateProviderData {{
          address
          reviewed
        }}
        isErc4626
        erc4626ReviewData {{
          summary
        }}
        priority
      }}
    }}
    """

    SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")

    logging.basicConfig(level=logging.INFO)
    logging.info("Starting the script...")

    # fetch
    response = requests.post(API_URL, json={"query": QUERY})
    if response.status_code != 200:
        logging.error(
            f"Query failed with code {response.status_code}. {response.text}"
        )
        return
    logging.info("Data fetched successfully from the API.")

    tokens = pd.DataFrame(response.json()["data"]["tokenGetTokens"])

    # --- Normalize nested dicts (vectorized, no row-wise apply) ---
    # columns: address, reviewed
    prp = pd.json_normalize(tokens["priceRateProviderData"])
    prp = prp.rename(
        columns={
            "address": "rateProviderAddress",
            "reviewed": "rateProviderReviewed",
        }
    )

    r4626 = pd.json_normalize(tokens["erc4626ReviewData"])  # column: summary
    r4626 = r4626.rename(columns={"summary": "erc4626ReviewSummary"})

    # concat side-by-side; json_normalize returns all-NaN rows where original was None
    tokens = pd.concat([tokens.drop(
        columns=["priceRateProviderData", "erc4626ReviewData"]), prp, r4626], axis=1)

    # --- Vectorized underlying lookups (no apply) ---
    is_erc4626_by_address = tokens.set_index("address")["isErc4626"].to_dict()
    symbol_by_address = tokens.set_index("address")["symbol"].to_dict()

    tokens["underlyingIsErc4626"] = tokens["underlyingTokenAddress"].map(
        is_erc4626_by_address).fillna(False).astype(bool)
    tokens["underlyingSymbol"] = tokens["underlyingTokenAddress"].map(
        symbol_by_address)

    # Google Sheets
    client = pygsheets.authorize(service_account_file=SERVICE_ACCOUNT_FILE)
    logging.info("Authorized with Google Sheets API.")

    sh = client.open(spreadsheet_name)
    wks = sh.worksheet_by_title(worksheet_name)
    wks.clear()
    wks.set_dataframe(tokens, (1, 1))
    logging.info("Data written to Google Sheets successfully.")


if __name__ == "__main__":
    main(
        "MAINNET",
        spreadsheet_name="Token list and pool proposal - Mainnet",
        worksheet_name="Token list",
    )
