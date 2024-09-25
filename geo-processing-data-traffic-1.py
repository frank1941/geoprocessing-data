import pandas as pd
import geopandas as gpd
import calendar
import os 
from pathlib import Path


def DataReportPerDay(df:pd.DataFrame,month:int, BASEDIR1:Path, BASEDIR2:Path, ):
    BASEDIR1 = r'E:\PROMTEL-IPN\RESULTADOS_TRAFICO\data_trafico_spark\Resultados'
    gdf = gpd.read_file(os.path.join(BASEDIR1,'Final_toESRI_QoSCategorias_1y2Trim_v2.geojson'))
    
    BASEDIR2 = r'E:\PROMTEL-IPN\RESULTADOS_TRAFICO\data_trafico_spark\RegionsPolygons_v1'
    gdfreg = gpd.read_file(os.path.join(BASEDIR2,'Regiones_MX_22102018_region.shp'))
    gdfreg.to_crs('epsg:4326',inplace=True)

    gdfMun = gdf[['Entidad_ID', 'Entidad_Name', 'Municipio_ID', 'Municipio_Name','geometry']].drop_duplicates(subset='Municipio_ID')
    gdfPerDay = gpd.GeoDataFrame(df,geometry=gpd.points_from_xy(x=df['cast(ctgs.longitud as decimal(9,5))'], y = df['cast(ctgs.latitud as decimal(9,5))'], crs='epsg:4326'))

    gdfJoined = gpd.sjoin(gdfPerDay,gdfMun,how='left')
    gdfJoined.drop('index_right',axis=1,inplace=True)
    
    gdfJoined['fecha'] = pd.to_datetime(gdfJoined['fecha'])
    gdfJoined['Trimestre'] = gdfJoined['fecha'].dt.quarter#.apply(lambda x: (month -1) //3,axis=1)
    gdfJoined['Trimestre'] = gdfJoined.apply(lambda x: str(x['Trimestre']) + ' T',axis=1)

    gdfJoined['Año'] = gdfJoined['fecha'].dt.year
    gdfJoined['Mes'] = calendar.month_name[month]
    gdfJoined['Semana'] = gdfJoined["fecha"].dt.week

    gdfJoinedReg = gpd.sjoin(gdfJoined,gdfreg,how='left')
    gdfJoinedReg.rename(columns={'Reg':'Region'},inplace=True)
    

    gdfJoinedReg['Tráfico_Total'] = gdfJoined['tl_thrp_bits_dl'] + gdfJoined['tl_thrp_bits_ul']

    print(gdfJoinedReg.columns)

    columns = ['altan_site_id', 'Entidad_ID', 'Entidad_Name', 'Municipio_ID', 'Municipio_Name',  'vendor',  'Año', 'Trimestre', 'Mes', 'Semana',
               'fecha',  'tl_thrp_bits_dl', 'tl_thrp_bits_ul','Tráfico_Total', 'Region' ]
    dfJoined = gdfJoinedReg[columns]
    dfJoined.rename(columns = {'altan_site_id':'Site_ID', 'vendor':'Vendor','Fecha':'Dia', 'tl_thrp_bits_dl':'Trafico_DL', 'tl_thrp_bits_ul':'Trafico_UL'},inplace=True)
    Name= 'TETraficoSitios2024'+calendar.month_name[month]
    dfJoined.to_csv(Name+'.csv',encoding='utf-8-sig')
    return dfJoined