# -*- coding: utf-8 -*-

import os
import datetime
import pandas as pd
from pandas import DataFrame

EXIT_CODE_SUCCESS = 0
EXIT_CODE_ERROR = 1
EXIT_CODE_ERROR_FILE_REQUIRED = 2
EXIT_CODE_ERROR_VALIDATION = 3

INPUT_DIR_BASE = '../../input/crossmall'
INPUT_PATH_TO_SKU = f'{INPUT_DIR_BASE}/2023AW_SKU.csv'
INPUT_PATH_TO_MBO = f'{INPUT_DIR_BASE}/2023AW_MBO.csv'
INPUT_PATH_TO_SALE_ITEMS = f'{INPUT_DIR_BASE}/0918_TAMARIS.xlsx'
OUTPUT_DIR_BASE = '../../output/crossmall'
OUTPUT_MASTER = f'{OUTPUT_DIR_BASE}/HARMONY_master_{datetime.datetime.now().strftime("%Y%m%d")}000000.tsv'
OUTPUT_STOCK = f'{OUTPUT_DIR_BASE}/HARMONY_stock_{datetime.datetime.now().strftime("%Y%m%d")}000000.tsv'
OUTPUT_SALESKU = f'{OUTPUT_DIR_BASE}/HARMONY_salesku_{datetime.datetime.now().strftime("%Y%m%d")}000000.tsv'

# 以下設定は実行の都度確認必要
CP_SALE_ID = 10152127
SALE_START_DATE = '20230918'
SALE_END_DATE = '20230922'
CROSSMALL_VENDOR_CODE = '311025910-crossmall'


def preprocess() -> DataFrame:
    try:
        if not os.path.exists(INPUT_PATH_TO_SKU):
            print('ファイルを配下に置いて下さい。\nファイル名の例: 2023AW_SKU.csv')
            exit(EXIT_CODE_ERROR_FILE_REQUIRED)
        if not os.path.exists(INPUT_PATH_TO_MBO):
            print('ファイルを配下に置いて下さい。\nファイル名の例: 2023AW_MBO.csv')
            exit(EXIT_CODE_ERROR_FILE_REQUIRED)
        if not os.path.exists(INPUT_PATH_TO_SALE_ITEMS):
            print('ファイルを配下に置いて下さい。\nファイル名の例: 0906_FABIO RUSCONI.xlsx')
            exit(EXIT_CODE_ERROR_FILE_REQUIRED)

        df_items = pd.read_csv(INPUT_PATH_TO_SKU, encoding='utf8')

        if '属性１コード' not in df_items.columns \
                or '属性２コード' not in df_items.columns:
            print('SKUファイルの中に「属性１コード/属性２コード」が含まれないのでファイルをチェックして下さい。')
            exit(EXIT_CODE_ERROR_VALIDATION)

        df_items['属性１コード'] = df_items['属性１コード'].apply(str)
        df_items['属性２コード'] = df_items['属性２コード'].apply(str)

        df_mbo = pd.read_csv(INPUT_PATH_TO_MBO, encoding='utf8')
        df_sale_items = pd.read_excel(INPUT_PATH_TO_SALE_ITEMS)

        if 'JANコード' not in df_items.columns \
                or 'JANコード' not in df_mbo.columns \
                or 'JANコード' not in df_sale_items.columns:
            print('入力ファイルのヘッダーの何れかに「JANコード」が含まれないのでファイルをチェックして下さい。')
            exit(EXIT_CODE_ERROR_VALIDATION)

        if '引当可能数' not in df_sale_items.columns:
            print('入力ファイルのヘッダーに「引当可能数」が含まれないのでファイルをチェックして下さい。')
            print('「引当可能数(9/1時点)」という名前で担当MDより共有されたりするのでヘッダ名を修正して下さい。')
            exit(EXIT_CODE_ERROR_VALIDATION)

        # 扱うファイルが増えた場合ここで調整すると良い ========================================================================
        df_base = pd.merge(df_items, df_mbo, on='JANコード', how='inner')
        df_base = pd.merge(df_base, df_sale_items, on='JANコード', how='inner')
        # ==============================================================================================================

        if '商品コード' not in df_base.columns \
                or '属性１コード' not in df_base.columns \
                or '属性２コード' not in df_base.columns:
            print('入力ファイルのヘッダーの何れかに「商品コード/属性１コード/属性２コード」が含まれないのでファイルをチェックして下さい。')
            exit(EXIT_CODE_ERROR_VALIDATION)

        # 全体で使い回す(TSVを跨ぐ)値を生成したい場合はここで実装すると良い ======================================================
        df_base['sku_code'] = df_base['商品コード'] + df_base['属性１コード'] + df_base['属性２コード']
        # ==============================================================================================================

        return df_base
    except Exception as e:
        print(f'実行時エラー: {e}')
        exit(EXIT_CODE_ERROR)


def master_tsv(df_base) -> list[DataFrame | str]:
    df_master = pd.DataFrame(index=range(df_base.shape[0]),
                             columns=[
                                 'vendor_code', 'sku_code', 'brand_code', 'brand_name', 'sub_brand_code',
                                 'sub_brand_name', 'vendor_item_code', 'sub_item_code', 'vendor_item_name',
                                 'product_code', 'product_model_number', 'product_name', 'planning_year',
                                 'season_code', 'sub_season_code', 'gender_code', 'long_comment', 'normal_comment',
                                 'short_comment', 'catch_phrase', 'search_keyword', 'accessory', 'main_color_code',
                                 'color_code', 'color_name', 'size_code', 'size_name', 'material_name',
                                 'producer_country', 'laundry_symbol', 'jancode', 'catalog_price', 'sales_tax_type',
                                 'fashion_model_info', 'sizing_name', 'sizing_value', 'sizing_unit', 'sizing_info',
                                 'explanatory_notes', 'demerit_name', 'demerit_tag', 'sales_type', 'sales_start_date',
                                 'arrival_date', 'sales_end_date', 'deleted'])
    df_master['vendor_code'] = CROSSMALL_VENDOR_CODE  # db column not null
    df_master['sku_code'] = df_base['sku_code']  # db column not null
    df_master['brand_code'] = df_base['ブランドコード']  # db column not null
    df_master['brand_name'] = df_base['商品名'].apply(lambda x: x.split('◆')[0])  # db column not null
    df_master['sub_brand_code'] = df_master['brand_code']  # null許容(「brand_code」と同じ値を入れる)
    df_master['jancode'] = df_base['JANコード']  # db column not null
    df_master['product_code'] = df_base['マルイ用ショップ型番']  # db column not null
    df_master['product_model_number'] = df_base['商品番号']  # db column not null
    df_master['product_name'] = df_base.apply(product_name, axis=1)  # db column not null
    df_master['long_comment'] = df_base['商品説明文(1)'].apply(lambda x: x.replace('\n', ''))  # null許容
    df_master['main_color_code'] = df_base['ショップ横軸']  # db column not null
    df_master['color_code'] = df_base['ショップ横軸']  # db column not null
    df_master['color_name'] = df_base['横軸名称']  # db column not null
    df_master['size_code'] = df_base['ショップ縦軸']  # db column not null
    df_master['size_name'] = df_base['縦軸名称']  # db column not null
    df_master['material_name'] = df_base['素材'].apply(lambda x: x.replace('"', '').replace('\n', ''))  # null許容 改行とかはとりあえず取る
    df_master['producer_country'] = df_base['原産国']  # null許容
    df_master['catalog_price'] = df_base['Ｐ上代']
    df_master['sub_brand_name'] = df_master['brand_name']  # null許容(「brand_name」と同じ値を入れる)
    df_master['planning_year'] = 2023
    df_master['season_code'] = '01'
    df_master['gender_code'] = 2
    df_master['normal_comment'] = df_master['long_comment']
    df_master['sales_tax_type'] = 0  # 0以外を選ぶと在庫連携の既存バグにより金額計算がおかしくなる可能性あり
    df_master['sales_type'] = 1
    df_master['sales_start_date'] = SALE_START_DATE
    df_master['sales_end_date'] = SALE_END_DATE
    df_master['deleted'] = 0
    return [df_master, OUTPUT_MASTER]


def product_name(row) -> str:
    delimiter_count = row['商品名'].count('◆')
    if delimiter_count == 1:
        return row['商品名'].split('◆')[1]
    elif delimiter_count == 2:
        return row['商品名'].split('◆')[1] + ' ' + row['商品名'].split('◆')[2]
    else:
        print('想定していない「商品名」のフォーマットです。ファイルを確認してください。')
        exit(EXIT_CODE_ERROR_VALIDATION)


def stock_tsv(df_base) -> list[DataFrame | str]:
    df_stock = pd.DataFrame(index=range(df_base.shape[0]),
                            columns=['vendor_code', 'sku_code', 'sales_type', 'quantity'])
    df_stock['vendor_code'] = CROSSMALL_VENDOR_CODE
    df_stock['sku_code'] = df_base['sku_code']
    df_stock['sales_type'] = 1
    df_stock['quantity'] = df_base['引当可能数']
    return [df_stock, OUTPUT_STOCK]


def salesku_tsv(df_base) -> list[DataFrame | str]:
    df_salesku = pd.DataFrame(index=range(df_base.shape[0]),
                              columns=['cp_sale_id', 'sale_start_date', 'vendor_code', 'sku_code', 'sale_price',
                                       'deleted'])
    df_salesku['cp_sale_id'] = CP_SALE_ID
    df_salesku['sale_start_date'] = SALE_START_DATE
    df_salesku['vendor_code'] = CROSSMALL_VENDOR_CODE
    df_salesku['sku_code'] = df_base['sku_code']
    df_salesku['deleted'] = 0
    df_salesku['sale_price'] = df_base['FLASHSALE価格']
    return [df_salesku, OUTPUT_SALESKU]


def output(*dfs) -> None:
    for df, path in dfs:
        df.to_csv(path, sep='\t', index=False)


if __name__ == '__main__':
    df_base: DataFrame = preprocess()
    output(master_tsv(df_base), stock_tsv(df_base), salesku_tsv(df_base))
    exit(EXIT_CODE_SUCCESS)
