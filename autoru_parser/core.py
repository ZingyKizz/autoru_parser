import json
import time
import six
import collections
import datetime
import pandas as pd
from functools import reduce
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from autoru_parser.retry_stuff import session_with_retries, response_post_json_with_retries


def get_cars_catalog(out_file=None):
    session = session_with_retries(HTMLSession())
    if out_file is None:
        to_file = False
    elif isinstance(out_file, six.string_types) and out_file:
        to_file = True
    else:
        raise TypeError('Filename should be a non-empty string')

    url = 'https://auto.ru/htmlsitemap/mark_model_catalog.html'
    response = session.get(url, verify=False)
    soup = BeautifulSoup(response.content, 'lxml')

    models = {}

    models_hrefs = soup.find('div', class_='sitemap').find_all('a', href=True)
    for model_href in models_hrefs:
        name = model_href.text

        url_path = model_href.get('href').strip('/').split('/')[-3:]
        search_params_keys = ['category', 'mark', 'model']
        search_params_values = [element.upper() if idx != 0 else element for idx, element in enumerate(url_path)]
        search_params = dict(zip(search_params_keys, search_params_values))

        models[name] = search_params

    if to_file:
        with open(out_file, 'w', encoding='utf-8') as outf:
            json.dump(models, outf, ensure_ascii=False, indent=4)
    else:
        return models


def get_offers(session, mark, model, vehicle_type='cars', year_from=None, year_to=None, condition='new', radius=1000,
               sleep=0.01, verbose=False, out_file=None):
    if not (
        (isinstance(mark, six.string_types) and mark) and
        (isinstance(model, six.string_types) and model)
    ):
        raise TypeError('mark and model arguments should be non-empty strings')

    mark_ = mark.strip().upper()
    model_ = model.strip().upper()

    if not (
        (isinstance(year_from, six.integer_types) or year_from is None) and
        (isinstance(year_to, six.integer_types) or year_to is None)
    ):
        raise TypeError('year_from and year_to arguments should be integers')

    min_year = 1890
    current_year = datetime.datetime.now().year

    if year_from is None and year_to is None:
        pass
    elif year_to is None:
        if not min_year <= year_from <= current_year:
            raise ValueError(f'year_from argument should lie between {min_year} and {current_year}')
    elif year_from is None:
        if not min_year <= year_to <= current_year:
            raise ValueError(f'year_to argument should lie between {min_year} and {current_year}')
    else:
        if not (
            min_year <= year_from <= current_year and
            min_year <= year_to <= current_year and
            year_from <= year_to
        ):
            raise ValueError('year_from argument should be less than or equal to year_to argument.'
                             f'And both of them should lie between {min_year} and {current_year}')

    if not isinstance(radius, six.integer_types):
        raise TypeError('radius argument should be an integer')

    if not 0 <= radius <= 1000:
        raise ValueError('radius argument should lie between 0 and 1000')

    if not isinstance(vehicle_type, six.string_types) and vehicle_type:
        raise TypeError('vehicle_type argument should be a non-empty string')

    vehicle_type_ = vehicle_type.strip().upper()
    vehicle_types = ('CARS', 'TRUCK', 'LCV', 'ARTIC', 'BUS', 'TRAILER', 'AGRICULTURAL', 'CONSTRUCTION', 'AUTOLOADER',
                     'CRANE', 'DREDGE', 'BULLDOZERS', 'MUNICIPAL')
    if vehicle_type_ not in vehicle_types:
        raise ValueError(f'Wrong vehicle_type argument value. Possible values are {vehicle_types}')

    if not isinstance(condition, six.string_types) and condition:
        raise TypeError('vehicle_type argument should be a non-empty string')

    condition_ = condition.strip().lower()
    conditions = ('new', 'used', 'all')
    if condition_ not in conditions:
        raise ValueError(f'Wrong condition argument value. Possible values are {conditions}')

    if not isinstance(verbose, bool):
        raise TypeError(f'verbose argument should be boolean')

    if out_file is None:
        to_file = False
    elif isinstance(out_file, six.string_types) and out_file:
        to_file = True
    else:
        raise TypeError('Filename should be a non-empty string')

    url = 'https://auto.ru/-/ajax/desktop/listing/'
    headers_dict = {'Host': 'auto.ru',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0',
                    'Accept': '*/*',
                    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Referer': 'https://auto.ru/',
                    'x-client-app-version': '202003.17.130516',
                    'x-page-request-id': '8b72cc0f6848aa9fb1eed42b433431ab',
                    'x-client-date': '1584533686542',
                    'x-csrf-token': 'f1be789f563580e2d377b188322555d495897488b944b0db',
                    'x-requested-with': 'fetch',
                    'content-type': 'application/json',
                    'Origin': 'https://auto.ru',
                    'Content-Length': '742',
                    'Connection': 'keep-alive',
                    'Cookie': '_csrf_token=f1be789f563580e2d377b188322555d495897488b944b0db; '
                              'autoru_sid=a%3Ag5e720a8f2eehbftl6p2i3ig1c5j8h33.416fd081fb24da206fe7536b324ef5fc'
                              '%7C1584532111399.604800.FXyNYRknlIn5MKYsF2QwDQ.bG6ficIde5b6B2'
                              '-oLrnoq1iKG1bF4usySCNkcT9fiBA '
                              '; autoruuid=g5e720a8f2eehbftl6p2i3ig1c5j8h33.416fd081fb24da206fe7536b324ef5fc; '
                              'suid=4ece150b49a5a43dd03878ee0c134ca7.e53207eb94576726e388045ee4ec978e; '
                              'from_lifetime=1584533666675; from=wizard; X-Vertis-DC=myt; '
                              'crookie=cjrUcMXOUYjMULZbLlYAxDPsTQBGUlDKL9c5zsM9qAPz1eLHTkhRe8wQ+C5qqqqQo+2aZ5'
                              '+1LBg61ZUMcj4Xpf99cz8=; cmtchd=MTU4NDUzMjExMjM2OA==; yandexuid=8775734681584532109; '
                              '_ym_wasSynced=%7B%22time%22%3A1584532114352%2C%22params%22%3A%7B%22eu%22%3A0%7D%2C'
                              '%22bkParams%22%3A%7B%7D%7D; gdpr=0; _ym_uid=1584532114271149300; _ym_d=1584533666; '
                              '_ym_visorc_22753222=b; _ym_isad=2; counter_ga_all7=2; _ym_visorc_148422=w; '
                              'cycada=RTBhKMFeBSr1Ixd+c/gseWA1lKSuD8M6rDi+9dja1Hk=; _ga=GA1.2.1476811577.1584533363; '
                              '_gid=GA1.2.1770866264.1584533363; _ym_visorc_148437=w; listing_view_session={}; '
                              'listing_view={%22output_type%22:%22table%22%2C%22version%22:1}; '
                              'navigation_promo_seen-recalls=true '
                    }

    params = {'catalog_filter': [{'mark': mark_, 'model': model_}],
              'section': condition_,
              'geo_radius': radius,
              'in_stock': 'IN_STOCK',
              'output_type': 'table',
              'sort': 'fresh_relevance_1-desc',
              'geo_id': [213]
              }
    if vehicle_type_ == 'CARS':
        params['category'] = 'cars'
    else:
        params['category'] = 'trucks'
        params['trucks_category'] = vehicle_type_
    if year_from:
        params['year_from'] = year_from
    if year_to:
        params['year_to'] = year_to

    offers_info = []
    current_page = last_page = 1

    def offer_info_deep_key(offer_path):
        key_name = offer_path[-1].lower()
        if key_name == 'engine_type':
            key_name = 'engine'
        elif key_name == 'gear_type':
            key_name = 'gear'
        elif key_name == 'power':
            key_name = 'horse_power'
        return key_name

    while current_page <= last_page:
        params['page'] = current_page
        try:
            response_json = response_post_json_with_retries(session, url, headers=headers_dict, json=params,
                                                            keys=['pagination', 'offers'])
        except ConnectionError as e:
            raise Exception(f'Connection error! Vehicle_type: {vehicle_type_}, mark: {mark_}, model: {model_}') from e

        if current_page == 1:
            last_page = response_json['pagination']['total_page_count']

        offers = response_json['offers']

        if vehicle_type_ == 'CARS':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'configuration', 'body_type'),
                ('vehicle_info', 'configuration', 'doors_count'),
                ('vehicle_info', 'configuration', 'auto_class'),
                ('vehicle_info', 'configuration', 'trunk_volume_min'),
                ('vehicle_info', 'steering_wheel'),
                ('vehicle_info', 'tech_param', 'engine_type'),
                ('vehicle_info', 'tech_param', 'gear_type'),
                ('vehicle_info', 'tech_param', 'transmission'),
                ('vehicle_info', 'tech_param', 'power'),
                ('vehicle_info', 'tech_param', 'fuel_rate'),
                ('vehicle_info', 'tech_param', 'acceleration')
            ]
            offers_path_num = (1, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1)

        elif vehicle_type_ == 'LCV':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'engine'),
                ('vehicle_info', 'gear'),
                ('vehicle_info', 'horse_power'),
                ('vehicle_info', 'loading'),
                ('vehicle_info', 'steering_wheel'),
                ('vehicle_info', 'transmission'),
                ('vehicle_info', 'seats')
            ]
            offers_path_num = (1, 1, 1, 0, 0, 1, 1, 0, 0, 1)

        elif vehicle_type_ == 'TRUCK':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'engine'),
                ('vehicle_info', 'euro_class'),
                ('vehicle_info', 'transmission'),
                ('vehicle_info', 'horse_power'),
                ('vehicle_info', 'loading'),
                ('vehicle_info', 'steering_wheel'),
                ('vehicle_info', 'wheel_drive')
            ]
            offers_path_num = (1, 1, 1, 0, 0, 0, 1, 1, 0, 0)

        elif vehicle_type_ == 'ARTIC':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'engine'),
                ('vehicle_info', 'transmission'),
                ('vehicle_info', 'horse_power'),
                ('vehicle_info', 'steering_wheel'),
                ('vehicle_info', 'wheel_drive')
            ]
            offers_path_num = (1, 1, 1, 0, 0, 1, 0, 0)

        elif vehicle_type_ == 'BUS':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'bus_type'),
                ('vehicle_info', 'engine'),
                ('vehicle_info', 'transmission'),
                ('vehicle_info', 'horse_power'),
                ('vehicle_info', 'steering_wheel'),
                ('vehicle_info', 'seats')
            ]
            offers_path_num = (1, 1, 1, 0, 0, 0, 1, 0, 1)

        elif vehicle_type_ == 'TRAILER':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'axis'),
                ('vehicle_info', 'loading')
            ]
            offers_path_num = (1, 1, 1, 1, 1)

        elif vehicle_type_ == 'AGRICULTURAL':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'agricultural_type'),
                ('vehicle_info', 'horse_power')
            ]
            offers_path_num = (1, 1, 1, 0, 1)

        elif vehicle_type_ == 'CONSTRUCTION':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'construction_type')
            ]
            offers_path_num = (1, 1, 1, 0)

        elif vehicle_type_ == 'AUTOLOADER':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'autoloader_type'),
                ('vehicle_info', 'load_height')
            ]
            offers_path_num = (1, 1, 1, 0, 1)

        elif vehicle_type_ == 'CRANE':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'crane_radius'),
                ('vehicle_info', 'loading'),
                ('vehicle_info', 'load_height')
            ]
            offers_path_num = (1, 1, 1, 1, 1, 1)

        elif vehicle_type_ == 'DREDGE':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'dredge_type'),
                ('vehicle_info', 'bucket_volume'),
                ('vehicle_info', 'horse_power')
            ]
            offers_path_num = (1,  1, 1, 0, 1, 1)

        elif vehicle_type_ == 'BULLDOZERS':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'bulldozer_type'),
                ('vehicle_info', 'horse_power')
            ]
            offers_path_num = (1, 1, 1, 0, 1)

        elif vehicle_type_ == 'MUNICIPAL':
            offers_paths = [
                ('price_info', 'RUR'),
                ('price_info', 'EUR'),
                ('price_info', 'USD'),
                ('vehicle_info', 'municipal_type'),
                ('vehicle_info', 'engine'),
                ('vehicle_info', 'horse_power')
            ]
            offers_path_num = (1, 1, 1, 0, 0, 1)

        for offer in offers:

            offer_info = {'num': {}, 'cat': {}}

            for is_num, offer_path in zip(offers_path_num, offers_paths):
                try:
                    offer_path_info = reduce(lambda x, y: x[y], offer_path, offer)
                    offer_info['num' if is_num else 'cat'][offer_info_deep_key(offer_path)] = offer_path_info
                except KeyError:
                    pass

            offers_info.append(offer_info)

        if verbose:
            print(f'| {mark:<15s} | {model:<30s} | {vehicle_type:<8s} | {current_page:>3} / {last_page:<3} page |')

        current_page += 1
        time.sleep(sleep)

    if to_file:
        with open(out_file, 'w', encoding='utf-8') as outf:
            json.dump(offers_info, outf, ensure_ascii=False, indent=4)
            print(f"\n'{out_file}' is ready")
    else:
        return offers_info


class Auto:
    def __init__(self, mark, model, vehicle_type, proxies=None):
        if not (
                (isinstance(mark, six.string_types) and mark) and
                (isinstance(model, six.string_types) and model)
        ):
            raise TypeError('mark and model arguments should be non-empty strings')
        self._mark = mark.strip().upper()
        self._model = model.strip().upper()

        if not isinstance(vehicle_type, six.string_types) and vehicle_type:
            raise TypeError('vehicle_type argument should be a non-empty string')
        vehicle_types = ('CARS', 'TRUCK', 'LCV', 'ARTIC', 'BUS', 'TRAILER', 'AGRICULTURAL', 'CONSTRUCTION',
                         'AUTOLOADER', 'CRANE', 'DREDGE', 'BULLDOZERS', 'MUNICIPAL')
        if vehicle_type.strip().upper() not in vehicle_types:
            raise ValueError(f'Wrong vehicle_type argument value. Possible values are {vehicle_types}')
        self._vehicle_type = vehicle_type.strip().upper()

        self._got_offers = False
        self._offers = None
        self._offers_description = None
        self._session = session_with_retries(HTMLSession())
        if proxies:
            if not isinstance(proxies, collections.Iterable):
                raise TypeError('proxies argument should be an iterable')
            self._session.trust_env = False
            self._session.proxies = proxies

    def get_offers(self, **kwargs):
        self._offers = get_offers(session=self._session, mark=self._mark, model=self._model,
                                  vehicle_type=self._vehicle_type, **kwargs)
        self._got_offers = True

        if not self._offers:
            self._offers_description = {}
        else:
            num_offers_info = pd.DataFrame([offer['num'] for offer in self._offers]).median().add_prefix('n_')
            cat_offers_info = pd.DataFrame([offer['cat'] for offer in self._offers]).mode().iloc[0].add_prefix('c_')
            self._offers_description = pd.concat([num_offers_info, cat_offers_info]).to_dict()

        return self


