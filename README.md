# Balancer API scripts

Set of tools and Python scripts to populate a given Google Sheets spreadsheet with information fetched from Balancer API.

## Google Cloud setup

1. Follow the [pygsheets documentation](https://pygsheets.readthedocs.io/en/stable/authorization.html) instructions. The method used in this repository is the **"Service Account"** one.

2. Create a folder called `auth` and move the downloaded `JSON` file to it.

3. Create a `.env` file and add the following line:

```
SERVICE_ACCOUNT_FILE=auth/your-auth-file.json
```

## Environment activation and running the project

1. Install uv via the instructions in the [offical uv documentation](https://docs.astral.sh/uv/getting-started/installation/).

2. Run the following commands:

```bash
$ uv sync --locked

$ source .venv/bin/activate # Linux or macOS

$ .venv\Scripts\activate # Windows
```

3. Adjust the `main()` function calling with your own spreadsheet and worksheet names:

```python
if __name__ == "__main__":
    main(
        spreadsheet_name="[your spreadsheet name]",
        worksheet_name="[your worksheet name]",
    )
```

4. Use `uv run` with the virtual environment activated to run the script. Example:

```bash
$ uv run get_token_list.py
```
