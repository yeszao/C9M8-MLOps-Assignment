from scripts.utils import build_dbs, load_data_into_db, map_city_tier, map_categorical_vars, interactions_mapping


def test_build_dbs():
    build_dbs()


def test_load_data_into_db():
    load_data_into_db()


def test_map_city_tier():
    map_city_tier()


def test_map_categorical_vars():
    map_categorical_vars()


def test_interactions_mapping():
    interactions_mapping()

