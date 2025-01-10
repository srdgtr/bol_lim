# elke maand draaien om te controleren of lim aadwerkelijk is verwerkt.

import argparse
import asyncio
from datetime import (
    datetime,
)
import io
import time
import httpx
import json
from configparser import (
    NoOptionError,
    ConfigParser,
)
from pathlib import (
    Path,
)
import pandas as pd
import requests
from sqlalchemy import MetaData, Table, create_engine, update
from sqlalchemy.engine.url import URL

ini_config = ConfigParser()
try:
    ini_config.read_file(open(Path.home() / "Dropbox" / "MACRO" / "bol_export_files.ini"))
except FileNotFoundError as e:
    ini_config.read(Path.home() / "bol_export_files.ini")

odin_db = dict(
    drivername="mariadb",
    username=ini_config.get("database odin", "user"),
    password=ini_config.get("database odin", "password"),
    host=ini_config.get("database odin", "host"),
    port=ini_config.get("database odin", "port"),
    database=ini_config.get("database odin", "database"),
)

engine = create_engine(URL.create(**odin_db))


class BOL_API:
    host = None
    key = None
    secret = None
    access_token = None
    access_token_expiration = None

    def __init__(
        self,
        host,
        key,
        secret,
    ):
        # set init values on creation
        self.host = host
        self.key = key
        self.secret = secret

        try:
            self.access_token = self.getAccessToken()
            if self.access_token is None:
                raise Exception("Request for access token failed.")
        except Exception as e:
            print(e)
        else:
            self.access_token_expiration = time.time() + 220

    def getAccessToken(
        self,
    ):
        # request the JWT
        try:
            # request an access token
            init_request = requests.post(
                self.host,
                auth=(
                    self.key,
                    self.secret,
                ),
            )
            init_request.raise_for_status()
        except Exception as e:
            print(e)
            return None
        else:
            token = json.loads(init_request.text)["access_token"]
            if token:  # add right headers
                post_header = {
                    "Accept": "application/vnd.retailer.v10+json",
                    "Content-Type": "application/vnd.retailer.v10+json",
                    "Authorization": "Bearer " + token,
                    "Connection": "keep-alive",
                }
            return post_header

    class Decorators:
        @staticmethod
        def refreshToken(
            decorated,
        ):
            # check the JWT and refresh if necessary
            def wrapper(
                api,
                *args,
                **kwargs,
            ):
                if time.time() > api.access_token_expiration:
                    api.access_token = api.getAccessToken()
                return decorated(
                    api,
                    *args,
                    **kwargs,
                )

            return wrapper

        @staticmethod
        def handle_url_exceptions(
            f,
        ):
            async def wrapper(
                *args,
                **kw,
            ):
                try:
                    return await f(
                        *args,
                        **kw,
                    )
                except httpx.HTTPStatusError as exc:
                    print(f"HTTPStatus response {exc.response.status_code} while requesting {exc.request.url!r}.")
                except httpx.ConnectError as e:
                    print(f">connectie fout naar bol {e.request.url}")
                except httpx.ConnectTimeout as e:
                    print(f"> timeout van bol api {e.request.url}")
                except httpx.ReadTimeout as e:
                    print(f"> ReadTimeout van bol api {e.request.url}")
                except httpx.HTTPError as exc:
                    print(f"HTTPError response {exc.response.status_code} while requesting {exc.request.url!r}.")

            return wrapper

    @Decorators.handle_url_exceptions
    @Decorators.refreshToken
    async def invoices_period(
        self,
        url,
    ):
        async with httpx.AsyncClient() as client:
            self.access_token["Accept"] = "application/vnd.retailer.v10+json"
            resp = await client.get(
                url,
                headers=self.access_token,
            )
            resp.raise_for_status()
            return resp

    @Decorators.handle_url_exceptions
    @Decorators.refreshToken
    async def specs_excel_info(
        self,
        url,
    ):
        timeout = httpx.Timeout(10.0, connect=30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            self.access_token[
                "Accept"
            ] = "application/vnd.retailer.v10+openxmlformats-officedocument.spreadsheetml.sheet"
            resp = await client.get(
                url,
                headers=self.access_token,
            )
            resp.raise_for_status()
            return resp

    @Decorators.handle_url_exceptions
    @Decorators.refreshToken
    async def specs_pdf_info(
        self,
        url,
    ):
        async with httpx.AsyncClient() as client:
            self.access_token["Accept"] = "application/vnd.retailer.v10+pdf"
            resp = await client.get(
                url,
                headers=self.access_token,
            )
            resp.raise_for_status()
            return resp


parser = argparse.ArgumentParser(
    description="voor facturen van een bepaalde maand, die zijn dus over vorige maand, dus 1 voor facturen december "
)
parser.add_argument("-m", default=False, help="waneer van een bepaalde maand")
parser.add_argument("-y", default=False, help="waneer van een bepaalde jaar, ook maand nodig")
args = parser.parse_args()

# :02d omdat we nummer met padding nodig hebben voor api
if args.m and args.y:
    factuur_periode_start = f"{args.y}-{int(args.m):02d}-01"
    factuur_periode_end = (
        f"{args.y}-{int(args.m):02d}-{(datetime(int(args.y), int(args.m), 1)+pd.offsets.MonthEnd(1)).strftime('%d')}"
    )
elif args.m and not args.y:
    factuur_periode_start = f"{datetime.today().year}-{int(args.m):02d}-01"
    factuur_periode_end = f"{datetime.today().year}-{int(args.m):02d}-{(datetime(datetime.today().year, int(args.m), 1)+pd.offsets.MonthEnd(1)).strftime('%d')}"
else:
    begin_date_range = pd.date_range(
        datetime.now() - pd.DateOffset(months=1),
        datetime.now(),
        freq="SMS",
    )
    eind_range = begin_date_range + pd.DateOffset(days=20)
    factuur_periode_start = begin_date_range.strftime("%Y-%m-%d")[::2].values[0]
    factuur_periode_end = eind_range.strftime("%Y-%m-%d")[::2].values[0]

winkel = {"all_day_elektro": "_ADE", "toop_bv": "_TB", "tp_shopper": "_TS", "typisch_elektro": "_TE"}

for webwinkel in ini_config["bol_winkels_api"]:
    client_id, client_secret, _, _ = [x.strip() for x in ini_config.get("bol_winkels_api", webwinkel).split(",")]
    bol_api_call = BOL_API(
        ini_config["bol_api_urls"]["authorize_url"],
        client_id,
        client_secret,
    )
    url = f"{ini_config['bol_api_urls']['base_url']}/invoices?period-start-date={factuur_periode_start}&period-end-date={factuur_periode_end}"
    print(url)
    factuur_nummers_info = asyncio.run(bol_api_call.invoices_period(url))
    if factuur_nummers_info:
        factuur_nummers_info_json = json.loads(factuur_nummers_info.text)
        aantal_facturen = len(factuur_nummers_info_json.get("invoiceListItems"))
        for factuur in range(aantal_facturen):
            try:
                factuur_nummer = factuur_nummers_info_json.get("invoiceListItems")[factuur].get("invoiceId")
            except IndexError:
                factuur_nummer = None
                print("geen factuurnummer(s) voor deze periode")
            if factuur_nummer:
                spec_exl_url = f"{ini_config['bol_api_urls']['base_url']}/invoices/{factuur_nummer}/specification?page="
                factuur_specs_info_exl = asyncio.run(bol_api_call.specs_excel_info(spec_exl_url))
                if factuur_specs_info_exl:
                    # zo krijg ik de goede maanden er bij, is niet echt logisch, maar lijkt te werken.
                    if factuur == 0 and aantal_facturen == 1:
                        file_excel = f"{webwinkel}_{factuur_nummer}_{(datetime.strptime(factuur_periode_start,'%Y-%m-%d')).strftime('%B')}"
                    elif factuur == 0 and aantal_facturen == 2:
                        file_excel = f"{webwinkel}_{factuur_nummer}_{(datetime.strptime(factuur_periode_start,'%Y-%m-%d')).strftime('%B')}"
                    elif factuur == 1:
                        file_excel = f"{webwinkel}_{factuur_nummer}_{(datetime.strptime(factuur_periode_start,'%Y-%m-%d')).strftime('%B')}"
                    with open(
                        f"{file_excel}.xlsx",
                        "wb",
                    ) as f:
                        f.write(factuur_specs_info_exl.content)

                    excel_file = (
                        pd.read_excel(io.BytesIO(factuur_specs_info_exl.content))
                        .rename(
                            columns={
                                "Uitleg over de factuur en de specificatie is te vinden op het Partnerplatform.": "webshop",
                                "Unnamed: 2": "Bestelnummer",
                                "Unnamed: 4": "EAN",
                                "Unnamed: 5": "Datum",
                                "Unnamed: 9": "Bedrag",
                            }
                        )
                        .assign(
                            Bedrag=lambda x: pd.to_numeric(
                                x["Bedrag"],
                                errors="coerce",
                            ),
                        )
                    )
                    winkel_short = winkel.get(webwinkel)
                    metadata = MetaData()
                    orders_info_bol = Table("orders_info_bol", metadata, autoload_with=engine)
                    for row in (
                        excel_file.query(
                            f"`webshop`=='Compensatie' or `webshop`=='Compensatie zoekgeraakte artikel(en)'"
                        )
                        .itertuples()
                    ):
                        update_lim_vergoed = (
                        update(orders_info_bol)
                        .where(orders_info_bol.columns.orderid == f"{row.Bestelnummer}{winkel_short}")
                        .values(lim_vergoed = 1,lim_vergoed_bedrag = row.Bedrag, lim_vergoed_date = row.Datum)
                        )
                        with engine.begin() as conn:
                            conn.execute(update_lim_vergoed)
                    for row in (
                        excel_file.query(
                            f"`webshop`=='Correctie verkoopprijs artikel(en)'"
                        )
                        .itertuples()
                    ):
                        update_return_lim_vergoed = (
                        update(orders_info_bol)
                        .where(orders_info_bol.columns.orderid == f"{row.Bestelnummer}{winkel_short}")
                        .values(return_vergoeding = 1,return_vergoeding_bedrag = row.Bedrag, return_vergoeding_date = row.Datum)
                        )
                        with engine.begin() as conn:
                            conn.execute(update_return_lim_vergoed)