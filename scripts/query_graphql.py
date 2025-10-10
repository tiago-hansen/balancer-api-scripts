import requests
import pandas as pd

url = "https://api-v3.balancer.fi"

query = """
{
	tokenGetTokens(
    chains: [MAINNET]
  ) {
    chain
    symbol
    underlyingTokenAddress
    address
    priceRateProviderData {
      address
      reviewed
    }
    websiteUrl
    isErc4626
    erc4626ReviewData {
      summary
    }
    priority
  }
}
"""

response = requests.post(
    url,
    json={'query': query}
)

# Print the result
# print(response.json())


# Save the result to a file
with open('tokens_raw.csv', 'w') as f:
    f.write("chain,symbol,underlyingTokenAddress,address,priceRateProviderData_address,priceRateProviderData_reviewed,websiteUrl,isErc4626,erc4626ReviewData_summary,priority\n")
    for token in response.json()['data']['tokenGetTokens']:
        f.write(
            f"{token['chain']},\
{token['symbol']},\
{token['underlyingTokenAddress']},\
{token['address']},\
{token['priceRateProviderData']['address'] if token['priceRateProviderData'] else ''},\
{token['priceRateProviderData']['reviewed'] if token['priceRateProviderData'] else ''},\
{token['websiteUrl']},\
{token['isErc4626']},\
{token['erc4626ReviewData']['summary'] if token['erc4626ReviewData'] else ''},\
{token['priority']}\n")

