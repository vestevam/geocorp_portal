import geopandas as gpd
from django.conf import settings
import datetime
from shapely import wkt
from pandas_gbq import to_gbq
from google.cloud import bigquery, storage

def upload(form_data):

    # Dados do Formulário
    arquivo = form_data['arquivo']
    tp_projeto = form_data['tp_projeto']
    nm_projeto = form_data['nm_projeto']

    # Ajustando dataframe para a tabela BQ
    gdf = gpd.read_file(arquivo, driver='KML')
    gdf = gdf.to_crs('OGC:CRS84')
    gdf['id'] = range(len(gdf))
    gdf["ct_mancha"] = tp_projeto
    gdf["nm_projeto"] = nm_projeto
    gdf["dt_ref"] = datetime.datetime.now().replace(day=1)
    # Converter geometria para 2D (remove Z)
    gdf['geometry_2d'] = gdf.geometry.apply(lambda geom: wkt.loads(wkt.dumps(geom, output_dimension=2)))
    gdf["geometry_wkt"] = gdf['geometry_2d'].apply(lambda x: x.wkt)
    gdf.drop(columns=["geometry"], inplace=True)
    gdf = gdf.loc[:, ['dt_ref','id', "nm_projeto", 'ct_mancha','geometry_wkt']]

    to_gbq(
        gdf,
        "dm_temp.geocorp_projetos_2", 
        project_id="tim-sdbx-corporate-baa7",
        if_exists="append"
        )

def fazenda_ha(form_data):

    nm_projeto = form_data['nm_projeto']

    query = f"""
    EXPORT DATA OPTIONS(
        uri = 'gs://tim-sdbx-corporate-baa7-temp-data/geocorp/portal/fazendas/{nm_projeto}/resultado-*.parquet',
        format = 'PARQUET',
        compression = 'SNAPPY',
        overwrite = true
    ) AS
        WITH intersecoes AS (
        SELECT
            p.id,
            c.id as id_cobertura,
            -- Calcular geometria de interseção
            ST_Intersection(st_geogfromtext(p.geometry_wkt), st_geogfromtext(c.geometry_wkt)) AS geom_intersecao
        FROM dm_temp.geocorp_projetos_2 p
        JOIN `tim-sdbx-corporate-baa7.dm_temp.geocorp_mancha_tim_4g_bronze` c
            ON ST_Intersects(st_geogfromtext(p.geometry_wkt), safe.st_geogfromtext(c.geometry_wkt))
        WHERE 
            ST_Area(ST_Intersection(st_geogfromtext(p.geometry_wkt), safe.st_geogfromtext(c.geometry_wkt))) > 0 and
            nm_projeto = "{nm_projeto}"
        ),

        calculos_area AS (
        SELECT
            id,
            -- Área total do polígono (já em hectares)
            st_area(st_geogfromtext(p.geometry_wkt)) / 10000 as area_ha,
            -- Área de interseção em hectares
            SUM(ST_Area(geom_intersecao)) / 10000 AS area_cob_ha,
            -- Percentual de cobertura
            SAFE_DIVIDE(SUM(ST_Area(geom_intersecao)), sum(ST_Area(st_geogfromtext(p.geometry_wkt)))) * 100 AS percentual_coberto
        FROM intersecoes i
        JOIN dm_temp.geocorp_projetos_2 p USING (id)
        WHERE
            p.nm_projeto = "{nm_projeto}"
        GROUP BY all
        )

        -- Atualizar a tabela original com os resultados
        SELECT
        p.* except(dt_ref, geometry_wkt),
        st_geogfromtext(p.geometry_wkt) as geometry,
        st_area(st_geogfromtext(geometry_wkt)) / 10000 as area_ha,
        round(COALESCE(c.area_cob_ha, 0),2) AS area_coberta_ha,
        round(COALESCE(c.percentual_coberto, 0)) AS percentual_coberto
        FROM dm_temp.geocorp_projetos_2 p
        LEFT JOIN calculos_area c USING (id)
        WHERE
            nm_projeto = "{nm_projeto}"
        """
    
    # Inicializar o cliente do BigQuery
    client = bigquery.Client()
    
    client.query(query)





